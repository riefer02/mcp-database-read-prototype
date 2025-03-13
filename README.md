# Database Read MCP Server

A simple Model Context Protocol (MCP) server for read-only PostgreSQL database access.

## What It Does

- Allows AI assistants to read from your database
- Provides tools for listing tables, viewing schemas, and running read-only queries
- Prevents any write operations to your database

## Setup

1. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

2. Configure Cursor:
   Create `.cursor/mcp.json` with:

   ```json
   {
     "mcpServers": {
       "database-reader": {
         "command": "/path/to/python",
         "args": ["/path/to/database_read.py"],
         "env": {
           "PYTHONPATH": "/path/to/project",
           "PYTHONUNBUFFERED": "1",
           "DATABASE_URL": "postgresql://username:password@hostname:port/database_name"
         }
       }
     }
   }
   ```

   **Note**: Put your database connection string directly in the mcp.json file. The .env file is not needed when using Cursor.

## Available Tools

- **database_query**: Run read-only SQL queries
- **list_tables**: List all tables in the database
- **get_table_schema**: Get schema for a specific table
- **get_all_schemas**: Get schemas for all tables at once

## Notes

- SQLAlchemy requires `postgresql://` not `postgres://` in connection strings
- All paths in the Cursor config must be absolute
- Restart Cursor after adding the configuration
