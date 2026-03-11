#!/usr/bin/env python3
"""Run read-only ClickHouse queries from a Slack slash command."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping, Sequence

_LEADING_SQL_COMMENTS_RE = re.compile(r"\A(\s|--[^\n]*(\n|$)|/\*.*?\*/)+", re.DOTALL)
_FIRST_WORD_RE = re.compile(r"[A-Za-z_]+")
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_READ_ONLY_PREFIXES = {"select", "with", "show", "describe", "desc", "explain"}


@dataclass(frozen=True)
class ServiceConfig:
    slack_signing_secret: str
    clickhouse_host: str
    clickhouse_port: int = 8443
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
    clickhouse_secure: bool = True
    clickhouse_verify: bool = True
    clickhouse_connect_timeout: int = 10
    clickhouse_send_receive_timeout: int = 30
    slack_command_path: str = "/slack/commands"
    max_rows: int = 50
    max_execution_seconds: int = 30
    allow_unsafe_sql: bool = False

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        return cls(
            slack_signing_secret=required_env("SLACK_SIGNING_SECRET"),
            clickhouse_host=required_env("CLICKHOUSE_HOST"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            clickhouse_secure=parse_bool(os.getenv("CLICKHOUSE_SECURE"), default=True),
            clickhouse_verify=parse_bool(os.getenv("CLICKHOUSE_VERIFY"), default=True),
            clickhouse_connect_timeout=int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT", "10")),
            clickhouse_send_receive_timeout=int(os.getenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "30")),
            slack_command_path=os.getenv("SLACK_COMMAND_PATH", "/slack/commands"),
            max_rows=int(os.getenv("SLACK_MAX_ROWS", "50")),
            max_execution_seconds=int(os.getenv("SLACK_MAX_EXECUTION_SECONDS", "30")),
            allow_unsafe_sql=parse_bool(os.getenv("SLACK_ALLOW_UNSAFE_SQL"), default=False),
        )


def parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    return default


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_header(headers: Mapping[str, str], key: str) -> str:
    value = headers.get(key)
    if value is not None:
        return value
    return headers.get(key.lower(), "")


def verify_slack_signature(
    headers: Mapping[str, str],
    body: bytes,
    signing_secret: str,
    *,
    now: int | None = None,
    tolerance_seconds: int = 300,
) -> bool:
    timestamp = _get_header(headers, "X-Slack-Request-Timestamp")
    signature = _get_header(headers, "X-Slack-Signature")
    if not timestamp or not signature:
        return False

    try:
        parsed_timestamp = int(timestamp)
    except ValueError:
        return False

    now_ts = int(time.time()) if now is None else now
    if abs(now_ts - parsed_timestamp) > tolerance_seconds:
        return False

    payload = b"v0:" + timestamp.encode("utf-8") + b":" + body
    expected = (
        "v0="
        + hmac.new(
            signing_secret.encode("utf-8"),
            payload,
            hashlib.sha256,
        ).hexdigest()
    )
    return hmac.compare_digest(expected, signature)


def _strip_leading_comments(sql: str) -> str:
    stripped = sql.lstrip()
    while True:
        match = _LEADING_SQL_COMMENTS_RE.match(stripped)
        if not match:
            return stripped
        stripped = stripped[match.end() :].lstrip()


def contains_multiple_statements(sql: str) -> bool:
    candidate = _strip_leading_comments(sql).strip()
    if not candidate:
        return False

    while candidate.endswith(";"):
        candidate = candidate[:-1].rstrip()
    return ";" in candidate


def is_read_only_sql(sql: str) -> bool:
    candidate = _strip_leading_comments(sql)
    match = _FIRST_WORD_RE.match(candidate)
    if not match:
        return False
    return match.group(0).lower() in _READ_ONLY_PREFIXES


def _format_value(value: Any, *, max_cell_chars: int = 80) -> str:
    text = "NULL" if value is None else str(value)
    normalized = text.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")
    if len(normalized) <= max_cell_chars:
        return normalized
    return normalized[: max_cell_chars - 1] + "…"


def render_text_table(
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    max_rows: int,
    max_table_chars: int = 2800,
) -> str:
    if not rows:
        return "_Query succeeded. No rows returned._"

    visible_rows = [list(row) for row in rows[:max_rows]]
    formatted = [[_format_value(cell) for cell in row] for row in visible_rows]
    widths = [len(column) for column in columns]
    for row in formatted:
        for i, value in enumerate(row):
            widths[i] = min(80, max(widths[i], len(value)))

    def row_to_line(row: Sequence[str]) -> str:
        padded = []
        for idx, value in enumerate(row):
            padded.append(value[: widths[idx]].ljust(widths[idx]))
        return " | ".join(padded)

    header = row_to_line(list(columns))
    divider = "-+-".join("-" * width for width in widths)
    lines = [header, divider]
    for row in formatted:
        lines.append(row_to_line(row))

    table = "```\n" + "\n".join(lines) + "\n```"
    if len(table) > max_table_chars:
        while len(table) > max_table_chars and len(lines) > 3:
            lines.pop()
            table = "```\n" + "\n".join(lines) + "\n```"
        table += "\n_Rows truncated to fit Slack message size._"
    elif len(rows) > max_rows:
        table += f"\n_Showing first {max_rows} rows._"
    return table


def post_slack_response(response_url: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        response_url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10):
        return


def execute_query_and_reply(sql: str, response_url: str, config: ServiceConfig) -> None:
    try:
        try:
            import clickhouse_connect
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing dependency: clickhouse-connect. "
                "Install it with `pip install clickhouse-connect`."
            ) from exc

        client = clickhouse_connect.get_client(
            host=config.clickhouse_host,
            port=config.clickhouse_port,
            username=config.clickhouse_user,
            password=config.clickhouse_password,
            database=config.clickhouse_database,
            interface="https" if config.clickhouse_secure else "http",
            verify=config.clickhouse_verify,
            connect_timeout=config.clickhouse_connect_timeout,
            send_receive_timeout=config.clickhouse_send_receive_timeout,
            query_limit=0,
        )
        try:
            result = client.query(
                sql,
                settings={
                    "max_result_rows": config.max_rows,
                    "result_overflow_mode": "break",
                    "max_execution_time": config.max_execution_seconds,
                },
            )
        finally:
            client.close()

        rendered = render_text_table(
            result.column_names,
            result.result_rows,
            max_rows=config.max_rows,
        )
        post_slack_response(
            response_url,
            {
                "response_type": "in_channel",
                "text": rendered,
            },
        )
    except Exception as exc:
        post_slack_response(
            response_url,
            {
                "response_type": "ephemeral",
                "text": f":x: Query failed: `{str(exc).strip()}`",
            },
        )


class SlackQueryHandler(BaseHTTPRequestHandler):
    config: ServiceConfig

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        self._write_json(HTTPStatus.OK, {"ok": True})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != self.config.slack_command_path:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        body = self._read_body()
        if not verify_slack_signature(self.headers, body, self.config.slack_signing_secret):
            self.send_error(HTTPStatus.UNAUTHORIZED, "Invalid Slack signature")
            return

        payload = urllib.parse.parse_qs(body.decode("utf-8"), keep_blank_values=True)
        sql = payload.get("text", [""])[0].strip()
        response_url = payload.get("response_url", [""])[0]

        if not sql:
            self._write_json(
                HTTPStatus.OK,
                {
                    "response_type": "ephemeral",
                    "text": "Usage: `/ch SELECT ...`",
                },
            )
            return

        if contains_multiple_statements(sql):
            self._write_json(
                HTTPStatus.OK,
                {
                    "response_type": "ephemeral",
                    "text": ":warning: Multiple SQL statements are not allowed.",
                },
            )
            return

        if not self.config.allow_unsafe_sql and not is_read_only_sql(sql):
            self._write_json(
                HTTPStatus.OK,
                {
                    "response_type": "ephemeral",
                    "text": (
                        ":warning: Only read-only queries are allowed "
                        "(SELECT/WITH/SHOW/DESCRIBE/EXPLAIN)."
                    ),
                },
            )
            return

        thread = threading.Thread(
            target=execute_query_and_reply,
            args=(sql, response_url, self.config),
            daemon=True,
        )
        thread.start()

        self._write_json(
            HTTPStatus.OK,
            {
                "response_type": "ephemeral",
                "text": ":hourglass_flowing_sand: Running query against ClickHouse...",
            },
        )

    def _read_body(self) -> bytes:
        content_length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(content_length)

    def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def make_handler(config: ServiceConfig) -> type[SlackQueryHandler]:
    class ConfiguredHandler(SlackQueryHandler):
        pass

    ConfiguredHandler.config = config
    return ConfiguredHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=3000, help="Bind port (default: 3000)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = ServiceConfig.from_env()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(config))
    print(
        f"Listening on http://{args.host}:{args.port}{config.slack_command_path} "
        "(health: /health)"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
