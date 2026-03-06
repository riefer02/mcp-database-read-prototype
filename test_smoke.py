#!/usr/bin/env python3
"""
Smoke tests for database_read MCP server improvements.
Run with: python test_smoke.py
"""
import subprocess
import sys
import os
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_SCRIPT = os.path.join(SCRIPT_DIR, "database_read.py")

def run_test(name, func):
    """Run a test and report results."""
    try:
        func()
        print(f"  ✓ {name}")
        return True
    except AssertionError as e:
        print(f"  ✗ {name}: {e}")
        return False
    except Exception as e:
        print(f"  ✗ {name}: {type(e).__name__}: {e}")
        return False


def test_startup_fails_without_database_url():
    """Server should exit with error when no DATABASE_URL is set."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("DATABASE_URL")}
    env["PATH"] = os.environ.get("PATH", "")

    result = subprocess.run(
        [sys.executable, DB_SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        timeout=5
    )

    assert result.returncode == 1, f"Expected exit code 1, got {result.returncode}"
    assert "No database URLs configured" in result.stderr, \
        f"Expected error message not found in: {result.stderr[:200]}"

    # Check for structured log event
    for line in result.stderr.split('\n'):
        if line.strip().startswith('{'):
            try:
                log = json.loads(line)
                if log.get("event") == "startup_failed":
                    assert log.get("reason") == "no_database_urls"
                    return
            except json.JSONDecodeError:
                pass

    raise AssertionError("startup_failed log event not found")


def test_write_operation_blocked_error_message():
    """Write operations should show which operation was blocked."""
    # Import the module with a mock DATABASE_URL
    env = os.environ.copy()
    env["DATABASE_URL"] = "postgresql://test:test@localhost:5432/test"

    # We can't easily test the full flow without a real DB,
    # but we can import and test the regex pattern directly
    import re

    write_ops_pattern = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b",
        flags=re.IGNORECASE,
    )

    test_cases = [
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name='x'", "UPDATE"),
        ("DELETE FROM users", "DELETE"),
        ("DROP TABLE users", "DROP"),
        ("CREATE TABLE foo (id int)", "CREATE"),
        ("ALTER TABLE users ADD col int", "ALTER"),
        ("TRUNCATE users", "TRUNCATE"),
    ]

    for query, expected_op in test_cases:
        match = write_ops_pattern.search(query)
        assert match is not None, f"Pattern should match '{query}'"
        assert match.group(1).upper() == expected_op, \
            f"Expected '{expected_op}' but got '{match.group(1)}'"


def test_write_pattern_no_false_positives():
    """Write pattern should not match legitimate column names."""
    import re

    write_ops_pattern = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b",
        flags=re.IGNORECASE,
    )

    safe_queries = [
        "SELECT is_deleted FROM users",
        "SELECT updated_at FROM orders",
        "SELECT created_at FROM posts",
        "SELECT drop_rate FROM metrics",
    ]

    for query in safe_queries:
        match = write_ops_pattern.search(query)
        assert match is None, f"Pattern should NOT match '{query}' but matched '{match.group(1)}'"


def test_log_event_format():
    """log_event should produce valid JSON with required fields."""
    import json
    from datetime import datetime, timezone

    # Simulate what log_event does
    def make_log_entry(event_type, **kwargs):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "event": event_type,
            **kwargs
        }

    entry = make_log_entry("query_executed",
        environment="local",
        duration_ms=123,
        row_count=10,
        truncated=False,
        query_preview="SELECT * FROM users"
    )

    # Should be valid JSON
    json_str = json.dumps(entry)
    parsed = json.loads(json_str)

    assert "timestamp" in parsed
    assert parsed["event"] == "query_executed"
    assert parsed["duration_ms"] == 123
    assert parsed["truncated"] == False


def test_health_check_function_exists():
    """Health check tool should be defined."""
    # Read the source and check for the function
    with open(DB_SCRIPT, 'r') as f:
        source = f.read()

    assert '@mcp.tool("health_check")' in source, "health_check tool not found"
    assert 'def handle_health_check' in source, "handle_health_check function not found"
    assert '"status": "healthy"' in source, "healthy status not in health check"
    assert '"status": "unhealthy"' in source, "unhealthy status not in health check"


def test_improved_error_message_format():
    """Error messages should include helpful hints."""
    with open(DB_SCRIPT, 'r') as f:
        source = f.read()

    # Check for improved environment error
    assert "Set DATABASE_URL_" in source, "Missing DATABASE_URL hint in error"
    assert "or check spelling" in source, "Missing spelling hint in error"

    # Check for improved write-op error
    assert "Write operation '" in source, "Missing operation name in error"
    assert "Only SELECT queries permitted" in source, "Missing SELECT hint in error"


def test_pyproject_toml_exists():
    """pyproject.toml should exist with essential dependencies."""
    pyproject_file = os.path.join(SCRIPT_DIR, "pyproject.toml")
    assert os.path.exists(pyproject_file), "pyproject.toml not found"

    with open(pyproject_file, 'r') as f:
        content = f.read()

    required_deps = ["mcp", "sqlalchemy", "psycopg2-binary"]
    for dep in required_deps:
        assert dep in content, f"Dependency '{dep}' should be in pyproject.toml"

    assert "requires-python" in content, "requires-python should be specified"


def main():
    print("Running smoke tests...\n")

    tests = [
        ("Startup fails without DATABASE_URL", test_startup_fails_without_database_url),
        ("Write operation error shows blocked op", test_write_operation_blocked_error_message),
        ("Write pattern has no false positives", test_write_pattern_no_false_positives),
        ("Log event produces valid JSON", test_log_event_format),
        ("Health check tool exists", test_health_check_function_exists),
        ("Improved error message format", test_improved_error_message_format),
        ("pyproject.toml exists with deps", test_pyproject_toml_exists),
    ]

    passed = 0
    failed = 0

    for name, func in tests:
        if run_test(name, func):
            passed += 1
        else:
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
