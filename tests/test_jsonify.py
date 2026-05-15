"""JSON-safe row serialization — no DB required."""
import base64
from datetime import datetime, date, time, timezone
from decimal import Decimal
from uuid import UUID

import database_read as db


def test_jsonify_primitives_passthrough():
    assert db._jsonify_value(None) is None
    assert db._jsonify_value(1) == 1
    assert db._jsonify_value(1.5) == 1.5
    assert db._jsonify_value(True) is True
    assert db._jsonify_value("x") == "x"


def test_jsonify_decimal():
    assert db._jsonify_value(Decimal("3.14")) == "3.14"
    assert db._jsonify_value(Decimal("0")) == "0"


def test_jsonify_uuid():
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert db._jsonify_value(u) == "12345678-1234-5678-1234-567812345678"


def test_jsonify_datetime():
    dt = datetime(2026, 5, 15, 10, 30, 45, tzinfo=timezone.utc)
    assert db._jsonify_value(dt) == "2026-05-15T10:30:45+00:00"


def test_jsonify_date():
    assert db._jsonify_value(date(2026, 5, 15)) == "2026-05-15"


def test_jsonify_time():
    assert db._jsonify_value(time(10, 30, 45)) == "10:30:45"


def test_jsonify_bytes():
    assert db._jsonify_value(b"hello") == base64.b64encode(b"hello").decode("ascii")


def test_jsonify_bytearray():
    assert db._jsonify_value(bytearray(b"hi")) == base64.b64encode(b"hi").decode("ascii")


def test_jsonify_memoryview():
    mv = memoryview(b"mem")
    assert db._jsonify_value(mv) == base64.b64encode(b"mem").decode("ascii")


def test_jsonify_list():
    assert db._jsonify_value([1, Decimal("2"), b"x"]) == [
        1, "2", base64.b64encode(b"x").decode("ascii")
    ]


def test_jsonify_dict():
    out = db._jsonify_value({"a": Decimal("1.1"), "b": [date(2026, 1, 1)]})
    assert out == {"a": "1.1", "b": ["2026-01-01"]}


def test_jsonify_row_dict():
    row = {"id": 1, "amount": Decimal("9.99"), "ts": date(2026, 5, 15)}
    out = db._jsonify_row(row)
    assert out == {"id": 1, "amount": "9.99", "ts": "2026-05-15"}


def test_jsonify_fallback_repr():
    class Weird:
        def __str__(self):
            return "weird-val"
    assert db._jsonify_value(Weird()) == "weird-val"
