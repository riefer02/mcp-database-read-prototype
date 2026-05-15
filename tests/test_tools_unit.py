"""MCP tool behaviors that don't need a live DB — error paths + validation paths.

Engine connect() is monkey-patched to raise so we exercise validator + error
surfacing without hitting Postgres.
"""
import pytest
import database_read as db


@pytest.fixture(autouse=True)
def _disable_engine(monkeypatch):
    """Force any engine-requiring path to surface a known error."""
    def boom(*a, **kw):
        raise RuntimeError("engine call attempted (test should not reach DB)")
    monkeypatch.setattr(db, "_get_engine", boom)


# --- database_query rejects writes BEFORE touching the engine ---

def test_database_query_blocks_insert_without_db():
    out = db.handle_database_query("INSERT INTO users VALUES (1)")
    assert out["status"] == "error"
    assert "INSERT" in out["message"]


def test_database_query_blocks_drop_without_db():
    out = db.handle_database_query("DROP TABLE users")
    assert out["status"] == "error"
    assert "DROP" in out["message"]


def test_database_query_blocks_multi_statement_without_db():
    out = db.handle_database_query("SELECT 1; DROP TABLE x")
    assert out["status"] == "error"
    assert "Multiple statements" in out["message"]


def test_database_query_rejects_negative_offset():
    out = db.handle_database_query("SELECT 1", offset=-1)
    assert out["status"] == "error"
    assert "offset" in out["message"]


def test_database_query_rejects_zero_max_rows():
    out = db.handle_database_query("SELECT 1", max_rows=0)
    assert out["status"] == "error"
    assert "max_rows" in out["message"]


# --- explain_query rejects writes ---

def test_explain_query_blocks_write():
    out = db.handle_explain_query("DELETE FROM users")
    assert out["status"] == "error"
    assert "DELETE" in out["message"]


# --- schema allowlist ---

def test_list_tables_rejects_outsider_schema():
    out = db.handle_list_tables(schema="information_schema")
    assert out["status"] == "error"
    assert "not in allowlist" in out["message"]


def test_get_table_schema_rejects_outsider_schema():
    out = db.handle_get_table_schema("users", schema="pg_catalog")
    assert out["status"] == "error"
    assert "not in allowlist" in out["message"]


def test_get_all_schemas_rejects_outsider_schema():
    out = db.handle_get_all_schemas(schema="information_schema")
    assert out["status"] == "error"
    assert "not in allowlist" in out["message"]
