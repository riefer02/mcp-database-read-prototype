"""
Read-only safety invariants.

This file is the canonical record of "we never write through this server."
It exercises the validator against every PG statement form that mutates
state, plus a defense-in-depth integration test that proves PG itself
rejects writes via SET TRANSACTION READ ONLY even if validation were
bypassed.

Documented limitations (NOT blocked):
- Function calls with side effects, e.g. `SELECT pg_terminate_backend(...)`.
  PG's READ ONLY transaction does not block these; protection there must
  come from the DB role used in DATABASE_URL.
- Cursor-based SELECTs do not pin row state — concurrent writers can move
  on; reads simply see their snapshot. That is by design.
"""
import pytest
import database_read as db


# ---------------------------------------------------------------------------
# Top-level statement starters that must be rejected before they reach PG.
# ---------------------------------------------------------------------------

NON_READ_STARTERS = [
    # Data writes
    ("INSERT INTO users VALUES (1)", "INSERT"),
    ("UPDATE users SET name='x'", "UPDATE"),
    ("DELETE FROM users", "DELETE"),
    ("MERGE INTO t USING s ON 1=1 WHEN MATCHED THEN UPDATE SET a=b", "MERGE"),
    # DDL
    ("CREATE TABLE t (id int)", "CREATE"),
    ("CREATE INDEX idx ON users (id)", "CREATE"),
    ("CREATE VIEW v AS SELECT 1", "CREATE"),
    ("CREATE MATERIALIZED VIEW mv AS SELECT 1", "CREATE"),
    ("CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE SQL", "CREATE"),
    ("ALTER TABLE users ADD col int", "ALTER"),
    ("ALTER ROLE me SUPERUSER", "ALTER"),
    ("DROP TABLE users", "DROP"),
    ("DROP DATABASE x", "DROP"),
    ("TRUNCATE users", "TRUNCATE"),
    # Transaction control / state
    ("BEGIN", None),
    ("COMMIT", None),
    ("ROLLBACK", None),
    ("SAVEPOINT s1", None),
    ("SET search_path TO public", None),
    ("RESET ALL", None),
    ("LOCK TABLE users", None),
    # Permissions
    ("GRANT SELECT ON users TO me", None),
    ("REVOKE SELECT ON users FROM me", None),
    # Postgres-specific data ops
    ("COPY users TO STDOUT", None),
    ("COPY users FROM STDIN", None),
    ("CALL my_proc()", None),
    ("DO $$ BEGIN PERFORM 1; END $$", None),
    ("VACUUM users", None),
    ("ANALYZE users", None),
    ("REINDEX TABLE users", None),
    ("CLUSTER users", None),
    ("COMMENT ON TABLE users IS 'x'", None),
    # Async / listening
    ("LISTEN ch", None),
    ("NOTIFY ch, 'msg'", None),
    ("UNLISTEN ch", None),
    # Replication / admin
    ("CHECKPOINT", None),
    # Bare EXPLAIN/SHOW are read-only-ish but not SELECT/WITH; route through
    # explain_query tool, not database_query
    ("EXPLAIN SELECT 1", None),
    ("SHOW search_path", None),
    ("VALUES (1)", None),
]


@pytest.mark.parametrize("sql,expected_substr", NON_READ_STARTERS)
def test_validator_rejects_non_read_starters(sql, expected_substr):
    with pytest.raises(ValueError) as exc:
        db.validate_read_only_sql(sql)
    if expected_substr is not None:
        assert expected_substr in str(exc.value)


# ---------------------------------------------------------------------------
# SELECT INTO is shorthand for CREATE TABLE AS — must be blocked.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT * INTO new_t FROM users",
    "SELECT id, name INTO new_t FROM users",
    "SELECT * INTO TEMP tmp FROM users",
    "SELECT * INTO UNLOGGED ul FROM users",
])
def test_validator_blocks_select_into(sql):
    with pytest.raises(ValueError, match="INTO"):
        db.validate_read_only_sql(sql)


def test_validator_blocks_select_into_inside_cte():
    with pytest.raises(ValueError, match="INTO"):
        db.validate_read_only_sql(
            "WITH q AS (SELECT 1) SELECT * INTO new_t FROM q"
        )


# ---------------------------------------------------------------------------
# CTE-style write attempts (writable CTEs are a real PG feature).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql,op", [
    ("WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x", "INSERT"),
    ("WITH x AS (UPDATE t SET a=1 RETURNING *) SELECT * FROM x", "UPDATE"),
    ("WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", "DELETE"),
    ("WITH x AS (MERGE INTO t USING s ON 1=1 WHEN MATCHED THEN UPDATE SET a=b RETURNING *) SELECT * FROM x", "MERGE"),
])
def test_validator_blocks_writable_cte(sql, op):
    with pytest.raises(ValueError, match=op):
        db.validate_read_only_sql(sql)


# ---------------------------------------------------------------------------
# Comment + multi-statement smuggling — every comment style x every form.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT 1; DROP TABLE x",
    "SELECT 1; INSERT INTO x VALUES (1)",
    "SELECT 1 /* comment */ ; DROP TABLE x",
    "SELECT 1 -- comment\n; DROP TABLE x",
    "SELECT 1; /* trailing */ DROP TABLE x",
    "SELECT 1 ;\n\n-- whitespace\nDELETE FROM y",
    "/* leading */ SELECT 1; DROP TABLE x",
    "SELECT 1;SELECT 2",  # two innocent selects still rejected — single-stmt only
])
def test_validator_rejects_multi_statement_variants(sql):
    with pytest.raises(ValueError):
        db.validate_read_only_sql(sql)


# ---------------------------------------------------------------------------
# Dangerous function calls — blocked at parse time because PG's READ ONLY
# transaction does NOT block these.
# ---------------------------------------------------------------------------

DANGEROUS_FN_QUERIES = [
    "SELECT pg_terminate_backend(123)",
    "SELECT pg_cancel_backend(123)",
    "SELECT pg_signal_backend(123, 15)",
    "SELECT pg_reload_conf()",
    "SELECT pg_rotate_logfile()",
    "SELECT pg_promote()",
    "SELECT pg_create_restore_point('x')",
    "SELECT pg_switch_wal()",
    "SELECT pg_read_file('postgresql.conf')",
    "SELECT pg_read_binary_file('foo')",
    "SELECT pg_ls_dir('/etc')",
    "SELECT pg_stat_file('foo')",
    "SELECT lo_unlink(1)",
    "SELECT lo_import('/tmp/x')",
    "SELECT lo_export(1, '/tmp/x')",
    "SELECT dblink('host=evil', 'SELECT 1')",
    "SELECT dblink_exec('host=evil', 'INSERT INTO x VALUES (1)')",
    "SELECT set_config('search_path', 'public', false)",
    "SELECT pg_export_snapshot()",
]


@pytest.mark.parametrize("sql", DANGEROUS_FN_QUERIES)
def test_validator_blocks_dangerous_functions(sql):
    with pytest.raises(ValueError, match="Dangerous function"):
        db.validate_read_only_sql(sql)


@pytest.mark.parametrize("sql", [
    "SELECT pg_catalog.pg_terminate_backend(123)",
    'SELECT "pg_terminate_backend"(123)',
    "SELECT PG_TERMINATE_BACKEND(123)",
    "SELECT pg_terminate_backend ( 123 )",
    "SELECT a, pg_terminate_backend(b), c FROM t",
    "WITH x AS (SELECT 1) SELECT pg_terminate_backend(123) FROM x",
    "SELECT * FROM t WHERE id = (SELECT pg_terminate_backend(1))",
])
def test_validator_blocks_dangerous_function_variants(sql):
    with pytest.raises(ValueError, match="Dangerous function"):
        db.validate_read_only_sql(sql)


@pytest.mark.parametrize("sql", [
    # String literal that contains the name — must NOT match
    "SELECT 'pg_terminate_backend(' AS s",
    "SELECT 'lo_unlink(' || 1 || ')' AS s",
    "SELECT $$pg_terminate_backend(1)$$ AS s",
    # Identifier reference (not a call) — must NOT match
    "SELECT pg_terminate_backend FROM funcs",
    "SELECT col AS pg_terminate_backend FROM t",
    # Similar name not on blacklist — must NOT match
    "SELECT pg_terminate_backend_v2(123)",
    "SELECT my_pg_terminate_backend(123)",
    # Read-only PG functions still work
    "SELECT pg_backend_pid()",
    "SELECT pg_sleep(0)",
    "SELECT current_database()",
    "SELECT version()",
])
def test_validator_allows_lookalikes_and_safe_funcs(sql):
    db.validate_read_only_sql(sql)  # no raise


# ---------------------------------------------------------------------------
# Strings / quoted identifiers MUST NOT trip the validator.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT 'DROP TABLE x' AS s",
    "SELECT 'INSERT INTO y' AS s, 1",
    "SELECT $$delete from z$$ AS s",
    'SELECT "delete_count" FROM stats',
    'SELECT "drop_rate" FROM metrics',
    "SELECT id FROM \"truncate_log\"",
    "SELECT id FROM create_dates",
    "SELECT is_deleted, updated_at, created_at FROM users",
])
def test_validator_allows_writes_inside_strings_and_identifiers(sql):
    db.validate_read_only_sql(sql)  # no raise


# ---------------------------------------------------------------------------
# Tool-level: ensure no tool path opens a write window.
# ---------------------------------------------------------------------------

@pytest.fixture
def patched_no_engine(monkeypatch):
    """Trip any engine call so we know validation happens first."""
    def boom(*a, **kw):
        raise RuntimeError("engine call attempted")
    monkeypatch.setattr(db, "_get_engine", boom)


@pytest.mark.parametrize("sql", [s for s, _ in NON_READ_STARTERS])
def test_database_query_tool_blocks_every_non_read_starter(sql, patched_no_engine):
    out = db.handle_database_query(sql)
    assert out["status"] == "error", f"tool accepted: {sql!r}"


@pytest.mark.parametrize("sql", [s for s, _ in NON_READ_STARTERS])
def test_explain_query_tool_blocks_every_non_read_starter(sql, patched_no_engine):
    out = db.handle_explain_query(sql)
    assert out["status"] == "error", f"explain accepted: {sql!r}"


# ---------------------------------------------------------------------------
# Defense in depth: even with validator bypassed, PG must reject the write
# because the transaction is SET TRANSACTION READ ONLY.
#
# Skipped unless MCP_TEST_DATABASE_URL is set.
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.integration


@pytest.mark.integration
def test_pg_read_only_blocks_write_if_validator_bypassed(mcp_env, fresh_schema, monkeypatch):
    """
    Patch out validate_read_only_sql so a write reaches the wrapped query.
    The wrapper (`SELECT * FROM (q) AS _mcp_sub`) makes most DML invalid as a
    subquery, but the key invariant is: SET TRANSACTION READ ONLY rejects
    any write that does make it through. Confirm both layers.
    """
    monkeypatch.setattr(mcp_env, "validate_read_only_sql", lambda _sql: None)
    out = mcp_env.handle_database_query(
        f"INSERT INTO \"{fresh_schema}\".widgets (name) VALUES ('intruder')"
    )
    assert out["status"] == "error"
    # PG should have refused either at syntax (cannot use INSERT as subquery)
    # or at read-only enforcement.
    msg = out["message"].lower()
    assert (
        "read-only" in msg
        or "syntax" in msg
        or "cannot execute" in msg
        or "permission" in msg
    ), f"unexpected error: {out['message']}"

    # And the data MUST be unchanged.
    count_out = mcp_env.handle_database_query(
        f'SELECT COUNT(*) AS c FROM "{fresh_schema}".widgets'
    )
    assert count_out["results"][0]["c"] == 3


@pytest.mark.integration
def test_pg_read_only_blocks_ddl_if_validator_bypassed(mcp_env, fresh_schema, monkeypatch):
    monkeypatch.setattr(mcp_env, "validate_read_only_sql", lambda _sql: None)
    out = mcp_env.handle_database_query(
        f'CREATE TABLE "{fresh_schema}".sneaky (id int)'
    )
    assert out["status"] == "error"

    # Confirm table not created
    tables = mcp_env.handle_list_tables(schema=fresh_schema)
    assert "sneaky" not in tables["tables"]


@pytest.mark.integration
def test_pg_read_only_blocks_truncate_if_validator_bypassed(mcp_env, fresh_schema, monkeypatch):
    monkeypatch.setattr(mcp_env, "validate_read_only_sql", lambda _sql: None)
    out = mcp_env.handle_database_query(
        f'TRUNCATE "{fresh_schema}".widgets'
    )
    assert out["status"] == "error"

    count_out = mcp_env.handle_database_query(
        f'SELECT COUNT(*) AS c FROM "{fresh_schema}".widgets'
    )
    assert count_out["results"][0]["c"] == 3


# ---------------------------------------------------------------------------
# Sanity: read-only queries that LOOK suspicious must still work.
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_read_query_with_write_keyword_in_string_works(mcp_env, fresh_schema):
    out = mcp_env.handle_database_query(
        f"SELECT 'INSERT INTO widgets' AS payload, COUNT(*) AS c FROM \"{fresh_schema}\".widgets"
    )
    assert out["status"] == "success"
    assert out["results"][0]["payload"] == "INSERT INTO widgets"
    assert out["results"][0]["c"] == 3


@pytest.mark.integration
def test_pg_catalog_reads_still_work(mcp_env):
    out = mcp_env.handle_database_query(
        "SELECT relname FROM pg_class WHERE relkind = 'r' LIMIT 1"
    )
    assert out["status"] == "success"


@pytest.mark.integration
def test_pg_terminate_backend_blocked_end_to_end(mcp_env):
    """Attempting to kill our own backend must be refused by the validator."""
    out = mcp_env.handle_database_query(
        "SELECT pg_terminate_backend(pg_backend_pid())"
    )
    assert out["status"] == "error"
    assert "Dangerous function" in out["message"]


@pytest.mark.integration
def test_safe_pg_funcs_still_work(mcp_env):
    """pg_backend_pid/pg_sleep/etc. are explicitly allowed."""
    out = mcp_env.handle_database_query(
        "SELECT pg_backend_pid() AS pid, current_database() AS db"
    )
    assert out["status"] == "success"
    assert isinstance(out["results"][0]["pid"], int)
