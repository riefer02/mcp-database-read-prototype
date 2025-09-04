from typing import Any, Dict, List, Optional
import os
import re
from sqlalchemy import create_engine, text
from mcp.server.fastmcp import FastMCP
import signal
import time

# Initialize FastMCP server
mcp = FastMCP("database_read")

# Constants
# Note: We had to change the URL from "postgres://" to "postgresql://" to work with SQLAlchemy
DATABASE_URL = os.getenv("DATABASE_URL")

# Safety configuration (env-overridable defaults)
# Best-practice defaults aimed at real-world reads while preventing runaway operations
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


# Initialize database connection with safe defaults
engine = (
    create_engine(
        DATABASE_URL,
        connect_args={
            "application_name": "mcp_read_only",
            # Apply baseline timeouts at connection level for defense-in-depth
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
    if DATABASE_URL
    else None
)


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
    max_rows: Optional[int] = None,
    statement_timeout_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query against the database and return the results.

    Args:
        query: SQL query string
        params: Optional parameters for the query

    Returns:
        List of dictionaries representing the query results
    """
    if engine is None:
        raise ValueError(
            "Database connection not configured. Please set DATABASE_URL in .env file."
        )

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
def get_table_names() -> List[str]:
    """
    Get a list of all table names in the database.

    Returns:
        List of table names
    """
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    """
    results = execute_query(query)
    return [row["table_name"] for row in results]


# Function to get table schema
def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
    """
    Get the schema for a specific table.

    Args:
        table_name: Name of the table

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
    return execute_query(query, {"table_name": table_name})


# Function to get primary key information
def get_primary_keys(table_name: str) -> List[str]:
    """
    Get primary key columns for a table.

    Args:
        table_name: Name of the table

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
    results = execute_query(query, {"table_name": table_name})
    return [row["column_name"] for row in results]


# Example MCP tool handler for database queries
@mcp.tool("database_query")
def handle_database_query(query: str) -> Dict[str, Any]:
    """
    MCP tool to execute a read-only database query.

    Args:
        query: SQL query to execute (SELECT statements only)

    Returns:
        Dictionary with query results
    """
    try:
        results = execute_query(query)
        truncated = len(results) >= DEFAULT_MAX_ROWS
        return {
            "status": "success",
            "results": results,
            "count": len(results),
            "truncated": truncated,
            "max_rows": DEFAULT_MAX_ROWS,
            "statement_timeout_ms": DEFAULT_STATEMENT_TIMEOUT_MS,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example MCP tool handler for listing tables
@mcp.tool("list_tables")
def handle_list_tables() -> Dict[str, Any]:
    """
    MCP tool to list all tables in the database.

    Returns:
        Dictionary with table names
    """
    try:
        tables = get_table_names()
        return {"status": "success", "tables": tables, "count": len(tables)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example MCP tool handler for getting table schema
@mcp.tool("get_table_schema")
def handle_get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    MCP tool to get the schema for a specific table.

    Args:
        table_name: Name of the table

    Returns:
        Dictionary with table schema information
    """
    try:
        schema = get_table_schema(table_name)
        primary_keys = get_primary_keys(table_name)

        return {
            "status": "success",
            "table": table_name,
            "schema": schema,
            "primary_keys": primary_keys,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool("get_all_schemas")
def handle_get_all_schemas() -> Dict[str, Any]:
    """
    MCP tool to get schemas for all tables in the database.
    This is useful for analyzing the entire database structure at once.

    Returns:
        Dictionary with schema information for all tables
    """
    try:
        tables = get_table_names()
        all_schemas = {}

        for table_name in tables:
            schema = get_table_schema(table_name)
            primary_keys = get_primary_keys(table_name)
            all_schemas[table_name] = {"schema": schema, "primary_keys": primary_keys}

            # Get a sample of data (first 5 rows) for each table
            try:
                sample_query = f'SELECT * FROM "{table_name}" LIMIT 5'
                sample_data = execute_query(sample_query)
                all_schemas[table_name]["sample_data"] = sample_data
            except Exception:
                all_schemas[table_name]["sample_data"] = []

        return {"status": "success", "table_count": len(tables), "schemas": all_schemas}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# If this file is run directly, start the MCP server
if __name__ == "__main__":
    print("Starting Database Read MCP Server...")
    print("Available tools:")
    print("  - database_query: Execute read-only SQL queries")
    print("  - list_tables: List all tables in the database")
    print("  - get_table_schema: Get schema for a specific table")
    print("  - get_all_schemas: Get schemas for all tables at once")
    # Use run method with explicit transport parameter for Cursor compatibility
    mcp.run(transport="stdio")
