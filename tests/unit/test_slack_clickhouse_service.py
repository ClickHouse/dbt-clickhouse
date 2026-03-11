import hashlib
import hmac

from slack_clickhouse_service import (
    contains_multiple_statements,
    is_read_only_sql,
    render_text_table,
    verify_slack_signature,
)


def test_is_read_only_sql() -> None:
    assert is_read_only_sql("SELECT 1")
    assert is_read_only_sql("  -- comment\nWITH x AS (SELECT 1) SELECT * FROM x")
    assert is_read_only_sql("EXPLAIN SELECT 1")
    assert not is_read_only_sql("INSERT INTO t VALUES (1)")


def test_contains_multiple_statements() -> None:
    assert contains_multiple_statements("SELECT 1; SELECT 2")
    assert not contains_multiple_statements("SELECT 1;")
    assert not contains_multiple_statements("  -- comment\nSELECT 1")


def test_render_text_table() -> None:
    output = render_text_table(["id", "name"], [[1, "alice"], [2, "bob"]], max_rows=1)
    assert "id | name" in output
    assert "1  | alice" in output
    assert "Showing first 1 rows." in output


def test_verify_slack_signature() -> None:
    body = b"command=%2Fch&text=SELECT+1"
    timestamp = "1710000000"
    secret = "my-secret"
    signature_payload = b"v0:" + timestamp.encode("utf-8") + b":" + body
    signature = (
        "v0="
        + hmac.new(
            secret.encode("utf-8"),
            signature_payload,
            hashlib.sha256,
        ).hexdigest()
    )
    headers = {
        "X-Slack-Request-Timestamp": timestamp,
        "X-Slack-Signature": signature,
    }

    assert verify_slack_signature(headers, body, secret, now=1710000001)
    assert not verify_slack_signature(headers, body, "wrong", now=1710000001)
