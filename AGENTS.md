# Agent Instructions for dbt-clickhouse

**[CONTRIBUTING.md](CONTRIBUTING.md) is the primary source of truth.** Read it before making any changes. This file only adds agent-specific workflow rules.

## General Rules

- **Never post to external systems** (GitHub PRs/issues, Slack, etc.) unless explicitly asked. "Review this PR" means analyze and report findings locally, not submit a review via `gh pr review`.

## Workflow

1. Read the files relevant to your task before editing. See CONTRIBUTING.md sections "Project Structure" and "Key Module Responsibilities" to locate them.
   - **dbt core source** is available at `.venv/lib/python3.12/site-packages/dbt/` — reference it when you need to understand dbt internals (materializations, adapters, contracts, compilation, etc.).
2. After every Python change: `make lint` (must pass) or `make fmt` (auto-fix).
3. Run the relevant tests for your change. See the "Test patterns by feature area" table in CONTRIBUTING.md for which tests to run.
4. If tests require ClickHouse and none is available, say so rather than skipping tests silently.

## File Location Quick Reference

This maps common tasks to files. For detailed guidance on each scenario, see "Making Changes: Common Scenarios" in CONTRIBUTING.md.

| Task | Files to Modify |
|---|---|
| Change how a materialization generates SQL | `dbt/include/clickhouse/macros/materializations/<type>.sql` |
| Change adapter behavior (connections, types, relations) | `dbt/adapters/clickhouse/impl.py`, `connections.py`, `relation.py` |
| Add/change a profile configuration option | `dbt/adapters/clickhouse/credentials.py` then wire through `connections.py` / `dbclient.py` |
| Fix column type mapping | `dbt/adapters/clickhouse/column.py` and/or `impl.py` |
| Fix ON CLUSTER behavior | `dbt/adapters/clickhouse/relation.py` + relevant macro |
| Fix driver-specific issues | `dbt/adapters/clickhouse/httpclient.py` or `nativeclient.py` |
| Fix MV dependency tracking | `dbt/adapters/clickhouse/cache.py` |
| Add utility SQL functions | `dbt/include/clickhouse/macros/utils.sql` or `datatypes.sql` |
| Update version | `dbt/adapters/clickhouse/__version__.py` |

## Agent-Specific Pitfalls

These are mistakes agents commonly make that aren't obvious from reading CONTRIBUTING.md:

- **Don't create `.sql` files for tests.** Integration tests define dbt models as multi-line Python strings passed via `@pytest.fixture(scope="class")`. See CONTRIBUTING.md "Writing Tests" for the pattern.
- **Always check for existing base test classes first.** Before writing tests from scratch, look in `.venv/lib/python3.12/site-packages/dbt/tests/adapter/` for base classes that already cover the feature. Extend them (pass-through, override fixtures, or override models as needed for ClickHouse). See CONTRIBUTING.md "Writing Tests" for the three patterns with examples.
- **Don't use `SET` statements in generated SQL.** ClickHouse session settings don't reliably persist across queries. Use the `custom_settings` profile option or `query_settings` model config instead.
- **Don't assume table rebuilds are a bug.** Table materialization intentionally drops and recreates on every `dbt run`. Only tables targeted by MVs have `mv_on_schema_change` protection.
- **New fields in `credentials.py` need defaults.** It's a frozen dataclass; omitting defaults breaks backwards compatibility.
- **Jinja whitespace matters.** Use `{%- -%}` for whitespace control to keep generated SQL clean.
- **Macro names prefixed with `clickhouse__` override dbt-core defaults** via `adapter.dispatch()`. Don't rename them without understanding the dispatch chain.
