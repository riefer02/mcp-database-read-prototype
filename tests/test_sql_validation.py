"""SQL validator tests — no DB required."""
import pytest
import database_read as db


# --- Allowed queries ---

@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select 1",
    "SELECT * FROM users",
    "SELECT id, name FROM users WHERE id > 5",
    "  SELECT 1  ",
    "SELECT * FROM users;",  # trailing semicolon stripped
    "SELECT * FROM users;   ",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "with x as (select 1) select * from x",
    "SELECT is_deleted FROM users",
    "SELECT updated_at FROM orders",
    "SELECT created_at FROM posts",
    "SELECT drop_rate FROM metrics",
    "SELECT * FROM create_dates",
    "SELECT * FROM information_schema.columns",
    "SELECT * FROM pg_stat_activity",
    "-- a comment\nSELECT 1",
    "/* leading block */ SELECT 1",
    "SELECT 'INSERT INTO foo' AS hint",  # write op inside string literal
    "SELECT \"insert_count\" FROM stats",  # quoted identifier
])
def test_validator_allows(sql):
    db.validate_read_only_sql(sql)


# --- Blocked queries ---

@pytest.mark.parametrize("sql,expected_op", [
    ("INSERT INTO users VALUES (1)", "INSERT"),
    ("insert into users values (1)", "INSERT"),
    ("UPDATE users SET name='x'", "UPDATE"),
    ("DELETE FROM users", "DELETE"),
    ("MERGE INTO target USING src ON 1=1 WHEN MATCHED THEN UPDATE SET a = b", "MERGE"),
])
def test_validator_blocks_write_dml(sql, expected_op):
    with pytest.raises(ValueError, match=expected_op):
        db.validate_read_only_sql(sql)


@pytest.mark.parametrize("sql,expected_op", [
    ("DROP TABLE users", "DROP"),
    ("CREATE TABLE foo (id int)", "CREATE"),
    ("ALTER TABLE users ADD col int", "ALTER"),
    ("TRUNCATE users", "TRUNCATE"),
])
def test_validator_blocks_ddl(sql, expected_op):
    with pytest.raises(ValueError, match=expected_op):
        db.validate_read_only_sql(sql)


def test_validator_blocks_multi_statement():
    with pytest.raises(ValueError, match="Multiple statements"):
        db.validate_read_only_sql("SELECT 1; SELECT 2")


def test_validator_blocks_select_then_drop():
    with pytest.raises(ValueError, match="Multiple statements"):
        db.validate_read_only_sql("SELECT 1; DROP TABLE x")


def test_validator_blocks_cte_with_write():
    with pytest.raises(ValueError, match="INSERT"):
        db.validate_read_only_sql(
            "WITH x AS (SELECT 1) INSERT INTO y SELECT * FROM x"
        )


def test_validator_blocks_cte_with_delete():
    with pytest.raises(ValueError, match="DELETE"):
        db.validate_read_only_sql(
            "WITH d AS (DELETE FROM x RETURNING *) SELECT * FROM d"
        )


def test_validator_rejects_empty():
    with pytest.raises(ValueError, match="Empty"):
        db.validate_read_only_sql("")


def test_validator_rejects_whitespace():
    with pytest.raises(ValueError, match="Empty"):
        db.validate_read_only_sql("   \n\t  ")


def test_validator_rejects_comment_only():
    with pytest.raises(ValueError):
        db.validate_read_only_sql("-- only a comment")


def test_validator_rejects_non_select_starter():
    with pytest.raises(ValueError, match="Only SELECT/WITH"):
        db.validate_read_only_sql("VACUUM ANALYZE")


def test_validator_rejects_show():
    with pytest.raises(ValueError, match="Only SELECT/WITH"):
        db.validate_read_only_sql("SHOW search_path")


def test_strip_trailing_semicolon():
    assert db._strip_trailing_semicolon("SELECT 1;") == "SELECT 1"
    assert db._strip_trailing_semicolon("SELECT 1; ") == "SELECT 1"
    assert db._strip_trailing_semicolon("SELECT 1") == "SELECT 1"
    # Does NOT strip interior semicolons — validator rejects those upstream
    assert "DROP" in db._strip_trailing_semicolon("SELECT 1; DROP TABLE x")


def test_wrap_select_with_limit_offset():
    wrapped = db._wrap_select_with_limit_offset("SELECT 1")
    assert ":_row_limit" in wrapped
    assert ":_row_offset" in wrapped
    assert "SELECT 1" in wrapped


def test_quote_ident_basic():
    assert db._quote_ident("users") == '"users"'
    assert db._quote_ident("Mixed_Case123") == '"Mixed_Case123"'


def test_quote_ident_rejects_dquote():
    with pytest.raises(ValueError):
        db._quote_ident('foo"bar')


def test_quote_ident_rejects_null():
    with pytest.raises(ValueError):
        db._quote_ident("foo\x00bar")


def test_quote_ident_rejects_empty():
    with pytest.raises(ValueError):
        db._quote_ident("")
