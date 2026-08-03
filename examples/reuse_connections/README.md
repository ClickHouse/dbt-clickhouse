# reuse_connections example

Example project used to validate the `reuse_connections` profile option (PR #670, issue #669).

It builds **many models in parallel** (tables + materialized views over those tables) and is run
twice — once with `reuse_connections: true` (default) and once with `reuse_connections: false` —
while a monkeypatch on `get_db_client` records how many distinct ClickHouse clients are created.

Expectation:

- `reuse_connections: true`  → one client per dbt thread (reused across all models on that thread)
- `reuse_connections: false` → one client per model (closed after every model)

Run the comparison harness:

```
python run_comparison.py
```

The project itself is run from the harness; you don't need to invoke `dbt` directly.
