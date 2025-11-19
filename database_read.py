from typing import Any, Dict, List, Optional
import os
import re
import sys
from sqlalchemy import create_engine, text
from mcp.server.fastmcp import FastMCP
import signal
import time

# Initialize FastMCP server
mcp = FastMCP("database_read")

# Safety configuration (env-overridable defaults). These defaults err on the side
# of protecting production systems from runaway read workloads.
DEFAULT_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "60000"))
DEFAULT_LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "15000"))
DEFAULT_IDLE_IN_TXN_TIMEOUT_MS = int(
    os.getenv("DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", "60000")
)

DEFAULT_MAX_ROWS = int(os.getenv("DB_MAX_ROWS", "10000"))
DEFAULT_FETCHMANY_SIZE = int(os.getenv("DB_FETCHMANY_SIZE", "1000"))

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "2"))
POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))

# Environment selection + pooling helpers
ENV_SELECTOR_VARS = ("DATABASE_TARGET_ENV", "DATABASE_ENV", "DB_ENV")
ENV_ALIAS_MAP = {
    "dev": "local",
    "development": "local",
    "stage": "staging",
    "stg": "staging",
    "prod": "production",
    "production": "production",
    "local": "local",
    "staging": "staging",
    "default": "default",
}
DATABASE_URL_PREFIX = "DATABASE_URL_"


def _normalize_env_name(env_name: Optional[str]) -> str:
    if not env_name:
        return "default"
    cleaned = env_name.strip().lower()
    return ENV_ALIAS_MAP.get(cleaned, cleaned)


def _discover_database_urls() -> Dict[str, str]:
    """
    Build a map of available database URLs discovered from environment variables.

    - `DATABASE_URL` becomes the implicit `default`
    - Any `DATABASE_URL_<ENV>` is registered under `<env>` (lowercase)
    """
    urls: Dict[str, str] = {}
    default_url = os.getenv("DATABASE_URL")
    if default_url:
        urls["default"] = default_url

    for key, value in os.environ.items():
        if not key.startswith(DATABASE_URL_PREFIX):
            continue
        suffix = key[len(DATABASE_URL_PREFIX) :].strip()
        if not suffix or not value:
            continue
        normalized = _normalize_env_name(suffix)
        urls[normalized] = value

    return urls


DATABASE_URLS = _discover_database_urls()
_ENGINE_CACHE: Dict[str, Any] = {}


def _available_envs_description() -> str:
    if not DATABASE_URLS:
        return "none configured"
    return ", ".join(sorted(DATABASE_URLS.keys()))


def _resolve_requested_environment(requested_env: Optional[str]) -> str:
    env_candidate = requested_env
    if not env_candidate:
        for var in ENV_SELECTOR_VARS:
            if os.getenv(var):
                env_candidate = os.getenv(var)
                break
    return _normalize_env_name(env_candidate)


def _create_engine(database_url: str):
    return create_engine(
        database_url,
        connect_args={
            "application_name": "mcp_read_only",
            "options": "-c statement_timeout={st} -c lock_timeout={lt} -c idle_in_transaction_session_timeout={it}".format(
                st=DEFAULT_STATEMENT_TIMEOUT_MS,
                lt=DEFAULT_LOCK_TIMEOUT_MS,
                it=DEFAULT_IDLE_IN_TXN_TIMEOUT_MS,
            ),
        },
        pool_pre_ping=True,
        pool_size=POOL_SIZE,
        max_overflow=MAX_OVERFLOW,
        pool_timeout=POOL_TIMEOUT,
        pool_recycle=POOL_RECYCLE,
    )


def _get_engine(requested_env: Optional[str] = None):
    """
    Lazily provision or reuse an engine for the requested environment.
    """
    target_env = _resolve_requested_environment(requested_env)
    database_url = DATABASE_URLS.get(target_env)

    if database_url is None:
        raise ValueError(
            f"No database URL configured for environment '{target_env}'. "
            f"Available environments: { _available_envs_description() }"
        )

    if target_env not in _ENGINE_CACHE:
        _ENGINE_CACHE[target_env] = _create_engine(database_url)

    return _ENGINE_CACHE[target_env]


class QueryCancelled(Exception):
    pass


def _install_cancellation_handlers():
    previous_int = signal.getsignal(signal.SIGINT)
    previous_term = signal.getsignal(signal.SIGTERM)

    def _cancel_handler(signum, frame):
        raise QueryCancelled("Operation cancelled by signal")

    signal.signal(signal.SIGINT, _cancel_handler)
    signal.signal(signal.SIGTERM, _cancel_handler)
    return previous_int, previous_term


def _restore_signal_handlers(prev_int, prev_term):
    signal.signal(signal.SIGINT, prev_int)
    signal.signal(signal.SIGTERM, prev_term)


def _strip_trailing_semicolon(sql: str) -> str:
    return re.sub(r";\s*$", "", sql)


def _wrap_select_with_limit(query: str, limit: int) -> str:
    """
    Wrap a SELECT/WITH query to enforce a hard LIMIT server-side.
    """
    inner = _strip_trailing_semicolon(query)
    return f"SELECT * FROM ({inner}) AS subquery LIMIT :_row_limit"


def _attempt_cancel(connection) -> None:
    """
    Best-effort cancel of the in-flight query at the driver level (psycopg2).
    Safe to call in error paths.
    """
    try:
        # SQLAlchemy Connection -> DBAPI connection is usually at .connection.connection
        dbapi_conn = getattr(
            getattr(connection, "connection", None), "connection", None
        )
        if dbapi_conn and hasattr(dbapi_conn, "cancel"):
            dbapi_conn.cancel()
    except Exception:
        # Swallow any errors – this is best-effort only
        pass


# Function to execute a SQL query and return results
def execute_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    environment: Optional[str] = None,
    max_rows: Optional[int] = None,
    statement_timeout_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query against the database and return the results.

    Args:
        query: SQL query string
        params: Optional parameters for the query
        environment: Optional environment label to run the query against
            (falls back to DATABASE_TARGET_ENV/DATABASE_ENV/DB_ENV, then default)

    Returns:
        List of dictionaries representing the query results
    """
    engine = _get_engine(environment)

    # Ensure query is read-only by checking for write operations using word boundaries
    # This prevents false positives like matching "DELETE" inside "is_deleted"
    write_ops_pattern = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b",
        flags=re.IGNORECASE,
    )
    if write_ops_pattern.search(query):
        raise ValueError("Only read operations are allowed")

    # Allow querying from information_schema and pg_ system catalogs
    query_upper = query.upper()
    is_system_query = "INFORMATION_SCHEMA" in query_upper or "PG_" in query_upper

    # Permit standard SELECT and WITH ... SELECT queries (reject WITH ... INSERT/DELETE/etc.)
    is_select_like = bool(
        re.match(r"^\s*(SELECT\b|WITH\b[\s\S]+?SELECT\b)", query, flags=re.IGNORECASE)
    )
    if not (is_system_query or is_select_like):
        raise ValueError(
            "Only SELECT operations and system catalog queries are allowed"
        )

    effective_max_rows = max_rows if max_rows is not None else DEFAULT_MAX_ROWS
    effective_timeout_ms = (
        statement_timeout_ms
        if statement_timeout_ms is not None
        else DEFAULT_STATEMENT_TIMEOUT_MS
    )

    # Server-side cap the result set
    safe_query = _wrap_select_with_limit(query, effective_max_rows)
    exec_params = dict(params or {})
    exec_params["_row_limit"] = effective_max_rows

    # Install cancellation handlers for graceful interruption
    prev_int, prev_term = _install_cancellation_handlers()

    with engine.connect() as connection:
        trans = connection.begin()
        result = None
        try:
            # Enforce read-only and strict timeouts inside the transaction
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(
                text("SET LOCAL statement_timeout = :timeout_ms"),
                {"timeout_ms": int(effective_timeout_ms)},
            )
            connection.execute(
                text("SET LOCAL lock_timeout = :lock_ms"),
                {"lock_ms": int(DEFAULT_LOCK_TIMEOUT_MS)},
            )
            connection.execute(
                text("SET LOCAL idle_in_transaction_session_timeout = :idle_ms"),
                {"idle_ms": int(DEFAULT_IDLE_IN_TXN_TIMEOUT_MS)},
            )

            result = connection.execution_options(stream_results=True).execute(
                text(safe_query), exec_params
            )

            rows: List[Dict[str, Any]] = []
            fetched = 0
            batch_size = max(1, DEFAULT_FETCHMANY_SIZE)

            # Wall-clock deadline as an extra safety net
            deadline_seconds = time.monotonic() + (int(effective_timeout_ms) / 1000.0)

            # Stream in batches to avoid memory blowups
            while True:
                if time.monotonic() > deadline_seconds:
                    raise TimeoutError(
                        "Client-side timeout exceeded while fetching results"
                    )

                batch = result.mappings().fetchmany(batch_size)
                if not batch:
                    break
                for row in batch:
                    rows.append(dict(row))
                    fetched += 1
                    if fetched >= effective_max_rows:
                        break
                if fetched >= effective_max_rows:
                    break

            trans.commit()
            return rows
        except (QueryCancelled, TimeoutError):
            try:
                _attempt_cancel(connection)
            finally:
                trans.rollback()
            raise
        except Exception:
            trans.rollback()
            raise
        finally:
            try:
                if result is not None:
                    result.close()
            finally:
                _restore_signal_handlers(prev_int, prev_term)


# Function to get table names from the database
def get_table_names(*, environment: Optional[str] = None) -> List[str]:
    """
    Get a list of all table names in the database.

    Args:
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        List of table names
    """
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    """
    results = execute_query(query, environment=environment)
    return [row["table_name"] for row in results]


# Function to get table schema
def get_table_schema(
    table_name: str, *, environment: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get the schema for a specific table.

    Args:
        table_name: Name of the table
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        List of dictionaries with column information
    """
    query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable,
        column_default,
        character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = :table_name
    ORDER BY ordinal_position
    """
    return execute_query(query, {"table_name": table_name}, environment=environment)


# Function to get primary key information
def get_primary_keys(
    table_name: str, *, environment: Optional[str] = None
) -> List[str]:
    """
    Get primary key columns for a table.

    Args:
        table_name: Name of the table
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        List of primary key column names
    """
    query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
        AND tc.table_name = :table_name
    ORDER BY kcu.ordinal_position
    """
    results = execute_query(query, {"table_name": table_name}, environment=environment)
    return [row["column_name"] for row in results]


# Example MCP tool handler for database queries
@mcp.tool("database_query")
def handle_database_query(query: str, environment: Optional[str] = None) -> Dict[str, Any]:
    """
    MCP tool to execute a read-only database query.

    Args:
        query: SQL query to execute (SELECT statements only)
        environment: Optional environment label to run against. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        Dictionary with query results
    """
    try:
        results = execute_query(query, environment=environment)
        truncated = len(results) >= DEFAULT_MAX_ROWS
        return {
            "status": "success",
            "results": results,
            "count": len(results),
            "truncated": truncated,
            "max_rows": DEFAULT_MAX_ROWS,
            "statement_timeout_ms": DEFAULT_STATEMENT_TIMEOUT_MS,
            "environment": _resolve_requested_environment(environment),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example MCP tool handler for listing tables
@mcp.tool("list_tables")
def handle_list_tables(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    MCP tool to list all tables in the database.

    Args:
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        Dictionary with table names
    """
    try:
        tables = get_table_names(environment=environment)
        return {"status": "success", "tables": tables, "count": len(tables)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example MCP tool handler for getting table schema
@mcp.tool("get_table_schema")
def handle_get_table_schema(
    table_name: str, environment: Optional[str] = None
) -> Dict[str, Any]:
    """
    MCP tool to get the schema for a specific table.

    Args:
        table_name: Name of the table
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        Dictionary with table schema information
    """
    try:
        schema = get_table_schema(table_name, environment=environment)
        primary_keys = get_primary_keys(table_name, environment=environment)

        return {
            "status": "success",
            "table": table_name,
            "schema": schema,
            "primary_keys": primary_keys,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool("get_all_schemas")
def handle_get_all_schemas(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    MCP tool to get schemas for all tables in the database.
    This is useful for analyzing the entire database structure at once.

    Args:
        environment: Optional environment label to inspect. When omitted,
            falls back to DATABASE_TARGET_ENV (and its aliases), then the
            default connection string.

    Returns:
        Dictionary with schema information for all tables
    """
    try:
        tables = get_table_names(environment=environment)
        all_schemas = {}

        for table_name in tables:
            schema = get_table_schema(table_name, environment=environment)
            primary_keys = get_primary_keys(table_name, environment=environment)
            all_schemas[table_name] = {"schema": schema, "primary_keys": primary_keys}

            # Get a sample of data (first 5 rows) for each table
            try:
                sample_query = f'SELECT * FROM "{table_name}" LIMIT 5'
                sample_data = execute_query(sample_query, environment=environment)
                all_schemas[table_name]["sample_data"] = sample_data
            except Exception:
                all_schemas[table_name]["sample_data"] = []

        return {"status": "success", "table_count": len(tables), "schemas": all_schemas}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# If this file is run directly, start the MCP server
if __name__ == "__main__":
    print("Starting Database Read MCP Server...", file=sys.stderr, flush=True)
    print("Available tools:", file=sys.stderr, flush=True)
    print("  - database_query: Execute read-only SQL queries", file=sys.stderr, flush=True)
    print("  - list_tables: List all tables in the database", file=sys.stderr, flush=True)
    print("  - get_table_schema: Get schema for a specific table", file=sys.stderr, flush=True)
    print("  - get_all_schemas: Get schemas for all tables at once", file=sys.stderr, flush=True)
    # Use run method with explicit transport parameter for Cursor compatibility
    mcp.run(transport="stdio")
