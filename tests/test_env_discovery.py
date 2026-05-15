"""Environment discovery + URL resolution — no DB required."""
import pytest
import database_read as db


def test_normalize_env_basic():
    assert db._normalize_env_name(None) == "default"
    assert db._normalize_env_name("") == "default"
    assert db._normalize_env_name("  ") == "default"
    assert db._normalize_env_name("LOCAL") == "local"
    assert db._normalize_env_name(" Staging ") == "staging"


def test_normalize_env_aliases():
    assert db._normalize_env_name("dev") == "local"
    assert db._normalize_env_name("development") == "local"
    assert db._normalize_env_name("stage") == "staging"
    assert db._normalize_env_name("stg") == "staging"
    assert db._normalize_env_name("prod") == "production"
    assert db._normalize_env_name("production") == "production"


def test_normalize_env_passthrough():
    # Unknown envs pass through lowercased
    assert db._normalize_env_name("qa") == "qa"
    assert db._normalize_env_name("EU_WEST") == "eu_west"


def test_discover_urls_default_only():
    env = {"DATABASE_URL": "postgresql://x/y"}
    urls = db._discover_database_urls(env)
    assert urls == {"default": "postgresql://x/y"}


def test_discover_urls_multiple():
    env = {
        "DATABASE_URL": "postgresql://d/d",
        "DATABASE_URL_LOCAL": "postgresql://l/l",
        "DATABASE_URL_STAGING": "postgresql://s/s",
        "DATABASE_URL_PROD": "postgresql://p/p",
    }
    urls = db._discover_database_urls(env)
    assert urls == {
        "default": "postgresql://d/d",
        "local": "postgresql://l/l",
        "staging": "postgresql://s/s",
        "production": "postgresql://p/p",
    }


def test_discover_urls_empty_value_skipped():
    env = {"DATABASE_URL_LOCAL": ""}
    assert db._discover_database_urls(env) == {}


def test_discover_urls_empty_suffix_skipped():
    # DATABASE_URL_ with empty suffix: not a real env
    env = {"DATABASE_URL_": "postgresql://x/y"}
    assert db._discover_database_urls(env) == {}


def test_discover_urls_ignores_unrelated():
    env = {"FOO": "bar", "DATABASE_URLS_PLURAL": "ignored"}
    assert db._discover_database_urls(env) == {}


def test_allowed_schemas_default():
    assert "public" in db.ALLOWED_SCHEMAS
    assert db.DEFAULT_SCHEMA == db.ALLOWED_SCHEMAS[0]


def test_parse_allowed_schemas():
    assert db._parse_allowed_schemas(None) == ("public",)
    assert db._parse_allowed_schemas("") == ("public",)
    assert db._parse_allowed_schemas("public") == ("public",)
    assert db._parse_allowed_schemas("public,reporting") == ("public", "reporting")
    assert db._parse_allowed_schemas(" public , reporting ") == ("public", "reporting")


def test_validate_schema_rejects_outsider():
    with pytest.raises(ValueError, match="not in allowlist"):
        db._validate_schema("information_schema")


def test_validate_schema_defaults():
    assert db._validate_schema(None) == db.DEFAULT_SCHEMA
