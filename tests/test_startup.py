"""Server-startup behavior tests via subprocess. No DB required."""
import json
import os
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = REPO / "database_read.py"


def _run(env: dict, timeout: float = 5.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        cwd=str(REPO),
    )


def test_startup_fails_without_database_url():
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABASE_URL")}
    env["PATH"] = os.environ.get("PATH", "")
    env["PYTHONPATH"] = str(REPO)
    result = _run(env)
    assert result.returncode == 1, result.stderr
    assert "No database URLs configured" in result.stderr


def test_startup_emits_structured_log_on_failure():
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABASE_URL")}
    env["PATH"] = os.environ.get("PATH", "")
    env["PYTHONPATH"] = str(REPO)
    result = _run(env)
    found = False
    for line in result.stderr.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            log = json.loads(line)
        except json.JSONDecodeError:
            continue
        if log.get("event") == "startup_failed":
            assert log.get("reason") == "no_database_urls"
            found = True
            break
    assert found, f"startup_failed log not found in stderr: {result.stderr[:500]}"


def test_error_message_helpful_hints():
    """Error message should mention DATABASE_URL_<ENV> + example."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABASE_URL")}
    env["PATH"] = os.environ.get("PATH", "")
    env["PYTHONPATH"] = str(REPO)
    result = _run(env)
    assert "DATABASE_URL_<ENV>" in result.stderr
    assert "DATABASE_URL_LOCAL" in result.stderr
