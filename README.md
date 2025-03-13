# Database Read MCP Server

A simple Model Context Protocol (MCP) server for read-only PostgreSQL database access.

## Quick Start

You need:

- Python 3.10+
- PostgreSQL database
- Cursor IDE

Steps:

1. Clone this repo
2. Run `pip install -r requirements.txt`
3. Create `.cursor/mcp.json` with your database connection:

   ```json
   {
     "mcpServers": {
       "database-reader": {
         "command": "/absolute/path/to/python",
         "args": ["/absolute/path/to/database_read.py"],
         "env": {
           "DATABASE_URL": "postgresql://username:password@hostname:port/database",
           "PYTHONUNBUFFERED": "1"
         }
       }
     }
   }
   ```

4. Restart Cursor
5. Ask the Agent to query your database

## What It Does

- Allows AI assistants to read from your database
- Provides tools for listing tables, viewing schemas, and running read-only queries
- Prevents any write operations to your database

## Available Tools

- **database_query**: Run read-only SQL queries
- **list_tables**: List all tables in the database
- **get_table_schema**: Get schema for a specific table
- **get_all_schemas**: Get schemas for all tables at once

## Important Notes

- SQLAlchemy requires `postgresql://` not `postgres://` in connection strings
- All paths in the Cursor config must be absolute
- Restart Cursor after adding the configuration
- The .env file is not needed when using with Cursor
