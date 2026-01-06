# Database Read MCP Server

A simple Model Context Protocol (MCP) server for read-only PostgreSQL database access.

## What you get

- **Read-only DB access** over MCP (stdio): list tables, fetch schemas, run `SELECT` queries
- **Write protection**: blocks `INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE`
- **Safety limits**: statement timeouts + hard row caps + streamed fetching

## Requirements

- Python 3.10+
- PostgreSQL database
- An MCP-compatible client (Cursor, Claude Desktop, Codex, etc.)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure the database connection

This server discovers database URLs from environment variables:

- **Default**: `DATABASE_URL`
- **Per-environment**: `DATABASE_URL_<ENV>` (e.g. `DATABASE_URL_LOCAL`, `DATABASE_URL_STAGING`)

Environment selection:

- **Default environment**: `DATABASE_TARGET_ENV` (also supports `DATABASE_ENV` or `DB_ENV`)
- **Per-tool override**: each MCP tool accepts optional `environment`

## Client setup

### Cursor

Cursor reads MCP config from either:

- **Project config**: `.cursor/mcp.json`
- **Global config**: `~/.cursor/mcp.json`

Example `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "database-reader": {
      "command": "${workspaceFolder}/.venv/bin/python",
      "args": ["${workspaceFolder}/database_read.py"],
      "env": {
        "PYTHONUNBUFFERED": "1",
        "DATABASE_TARGET_ENV": "local",
        "DATABASE_URL_LOCAL": "${env:DATABASE_URL_LOCAL}",
        "DATABASE_URL_STAGING": "${env:DATABASE_URL_STAGING}",
        "DATABASE_URL_PRODUCTION": "${env:DATABASE_URL_PRODUCTION}"
      }
    }
  }
}
```

Notes:

- Keep **real credentials out of git**; use `${env:...}` and set env vars in your shell/secret manager.
- Restart Cursor after editing MCP config.

### Claude Desktop

Claude Desktop reads MCP config from:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows (typical)**: `%AppData%\Claude\claude_desktop_config.json`

Example `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "database-reader": {
      "command": "/ABSOLUTE/PATH/TO/.venv/bin/python",
      "args": ["/ABSOLUTE/PATH/TO/mcp-prototype/database_read.py"],
      "env": {
        "PYTHONUNBUFFERED": "1",
        "DATABASE_TARGET_ENV": "local",
        "DATABASE_URL_LOCAL": "postgresql://user:password@localhost:5432/db_name"
      }
    }
  }
}
```

Notes:

- Claude Desktop commonly requires **absolute paths** in JSON.
- Restart Claude Desktop after editing the config.

### OpenAI Codex (CLI + IDE extension)

Codex shares MCP configuration between the CLI and IDE extension via:

- `~/.codex/config.toml`

Option A — configure via CLI:

```bash
codex mcp add database-reader \
  --env DATABASE_TARGET_ENV=local \
  --env DATABASE_URL_LOCAL='postgresql://user:password@localhost:5432/db_name' \
  -- python /ABSOLUTE/PATH/TO/mcp-prototype/database_read.py
```

Option B — configure via `~/.codex/config.toml`:

```toml
[mcp_servers.database-reader]
command = "python"
args = ["/ABSOLUTE/PATH/TO/mcp-prototype/database_read.py"]

[mcp_servers.database-reader.env]
PYTHONUNBUFFERED = "1"
DATABASE_TARGET_ENV = "local"
DATABASE_URL_LOCAL = "postgresql://user:password@localhost:5432/db_name"
```

### Other MCP clients (Claude Code, OpenCode, etc.)

If your client supports **STDIO MCP servers**, it will typically accept the same fields:

- `command`: executable to start the server (often `python`)
- `args`: arguments (the first arg is typically the path to `database_read.py`)
- `env`: environment variables (at least `DATABASE_URL` or `DATABASE_URL_<ENV>`)

Use the **Cursor / Claude Desktop JSON** examples above (same `mcpServers` shape), or the **Codex `config.toml`** example if your client uses TOML.

## Smoke test (works in any client)

Try these tool calls:

- **list_tables**: confirm you can see tables
- **get_table_schema**: pick one table and inspect columns + primary keys
- **database_query**: run a safe query like `SELECT * FROM some_table LIMIT 5`

To test multi-env selection, pass `environment: "staging"` in the tool call arguments (or change `DATABASE_TARGET_ENV`).

## Available Tools

- **database_query**: Run read-only SQL queries
- **list_tables**: List all tables in the database
- **get_table_schema**: Get schema for a specific table (includes primary keys)
- **get_all_schemas**: Get schemas for all tables (also attempts small sample data)

## Switching between environments

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

## Important Notes

- SQLAlchemy requires `postgresql://` not `postgres://` in connection strings
- Restart your client after adding the configuration
- Do not commit real credentials; use environment variables or a secret manager in production
