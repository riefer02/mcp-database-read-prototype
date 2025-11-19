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
           "PYTHONPATH": "/absolute/path/to/project",
           "PYTHONUNBUFFERED": "1",
           "MCP_SERVER_MODE": "stdio",
           "DATABASE_TARGET_ENV": "staging",
           "DATABASE_URL_LOCAL": "postgresql://local_user:local_pass@localhost:5432/local_db",
           "DATABASE_URL_STAGING": "postgresql://staging_user:staging_pass@staging-host:5432/staging_db",
           "DATABASE_URL_PRODUCTION": "postgresql://prod_user:prod_pass@prod-host:5432/prod_db"
         }
       }
     }
   }
   ```

4. Restart Cursor
5. Ask the Agent to query your database

## Switching Between Environments

- Define as many connection strings as you need via `DATABASE_URL_<ENV>` (e.g., `DATABASE_URL_LOCAL`, `DATABASE_URL_STAGING`, `DATABASE_URL_PRODUCTION`). `DATABASE_URL` still works as the default when no environment is specified.
- Tell the server which environment to use globally with `DATABASE_TARGET_ENV` (aliases such as `dev`, `prod`, `stage`, `stg`, `local`, `staging`, `production` are supported). You can also set `DATABASE_ENV` or `DB_ENV` if you prefer those names.
- Every MCP tool now accepts an optional `environment` argument, so you can ask the agent to run queries against a different database without editing your config. Example:

  ```json
  {
    "name": "database_query",
    "arguments": {
      "query": "SELECT * FROM users LIMIT 5",
      "environment": "staging"
    }
  }
  ```

- The server keeps its own safe connection pool per environment, so switching between local/staging/production reads does not require a restart.

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
