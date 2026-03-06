# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server providing read-only PostgreSQL database access. It runs as a stdio-based MCP server that integrates with AI assistants (Cursor, Claude Desktop, OpenAI Codex, etc.).

## Development Setup

```bash
uv sync
```

## Running the Server

```bash
uv run python database_read.py
```

The server communicates via stdio and requires database connection strings via environment variables:
- `DATABASE_URL` - default connection
- `DATABASE_URL_<ENV>` - per-environment connections (e.g., `DATABASE_URL_LOCAL`, `DATABASE_URL_STAGING`)
- `DATABASE_TARGET_ENV` - selects active environment (aliases: `DATABASE_ENV`, `DB_ENV`)

## Architecture

**Single-file architecture**: All server logic lives in `database_read.py` (~629 lines).

### Key Components

1. **FastMCP Server** (line 11): Uses `mcp.server.fastmcp.FastMCP` for MCP protocol handling

2. **Multi-environment support** (lines 29-131):
   - Discovers `DATABASE_URL_*` vars at startup
   - Maintains per-environment SQLAlchemy engine cache
   - Supports environment aliases (dev→local, prod→production, etc.)

3. **Query execution engine** (lines 184-308):
   - Regex-based write operation blocking (INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE)
   - All queries wrapped in `SELECT * FROM (query) LIMIT max_rows`
   - READ ONLY transaction mode
   - Statement/lock/idle timeouts via PostgreSQL SET commands
   - Stream-based result fetching with batch processing
   - Signal handlers (SIGINT/SIGTERM) for query cancellation

4. **MCP Tools** (lines 395-629):
   - `health_check`: Check database connectivity and server health
   - `database_query`: Execute SELECT queries with optional environment override
   - `list_tables`: List public schema tables
   - `get_table_schema`: Column metadata + primary keys for a table
   - `get_all_schemas`: All schemas + 5-row samples per table

### Safety Configuration (env vars)

| Variable | Default | Purpose |
|----------|---------|---------|
| `DB_STATEMENT_TIMEOUT_MS` | 60000 | Query timeout |
| `DB_LOCK_TIMEOUT_MS` | 15000 | Lock acquisition timeout |
| `DB_MAX_ROWS` | 10000 | Hard row limit per query |
| `DB_FETCHMANY_SIZE` | 1000 | Batch fetch size |
| `DB_POOL_SIZE` | 5 | Connection pool size |

## Testing

No formal test suite. Smoke test via MCP client:
1. `list_tables` - verify connection
2. `get_table_schema` - pick a table
3. `database_query` - run `SELECT * FROM table LIMIT 5`

## Key Dependencies

- `mcp[cli]>=1.26.0` - MCP protocol (FastMCP)
- `SQLAlchemy>=2.0.39` - Database abstraction
- `psycopg2-binary>=2.9.10` - PostgreSQL driver
