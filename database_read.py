from typing import Any, Dict, List
import os
import httpx
from sqlalchemy import create_engine, text
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("database_read")

# Constants
# Note: We had to change the URL from "postgres://" to "postgresql://" to work with SQLAlchemy
DATABASE_URL = os.getenv("DATABASE_URL")

# Initialize database connection
engine = (
    create_engine(DATABASE_URL, connect_args={"application_name": "mcp_read_only"})
    if DATABASE_URL
    else None
)


# Function to execute a SQL query and return results
def execute_query(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
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

    # Ensure query is read-only by checking for write operations
    query_upper = query.upper()
    if any(
        op in query_upper
        for op in ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
    ):
        raise ValueError("Only read operations are allowed")

    with engine.connect() as connection:
        result = connection.execute(text(query), params or {})
        # Convert result to list of dictionaries
        return [dict(row._mapping) for row in result]


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
        return {"status": "success", "results": results, "count": len(results)}
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
