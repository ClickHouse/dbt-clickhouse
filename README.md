<p align="center">
  <img src="https://raw.githubusercontent.com/ClickHouse/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

## Documentation

See the [ClickHouse website](https://clickhouse.com/docs/integrations/dbt) for the full documentation entry.

## Installation

Use your favorite Python package manager to install the app from PyPI, e.g.

```bash
pip install dbt-core dbt-clickhouse
```

> **_NOTE:_**  Beginning in v1.8, dbt-core and adapters are decoupled. Therefore, the installation mentioned above
> explicitly includes both dbt-core and the desired adapter.If you use a version prior to 1.8.0 the pip installation
> command should look like this:

```bash
pip install dbt-clickhouse
```

## Supported features

- [x] Table materialization
- [x] View materialization
- [x] Incremental materialization
- [x] Microbatch incremental materialization
- [x] Materialized View materializations (uses the `TO` form of MATERIALIZED VIEW, experimental)
- [x] Seeds
- [x] Sources
- [x] Docs generate
- [x] Tests
- [x] Snapshots
- [x] Most dbt-utils macros (now included in dbt-core)
- [x] Ephemeral materialization
- [x] Distributed table materialization (experimental)
- [x] Distributed incremental materialization (experimental)
- [x] Contracts
- [x] ClickHouse-specific column configurations (Codec, TTL...)
- [x] ClickHouse-specific table settings (indexes, projections...)

All features up to dbt-core 1.10 are supported, including `--sample` flag and all deprecation warnings fixed for future releases. **Catalog integrations** (e.g., Iceberg) introduced in dbt 1.10 are not yet natively supported in the adapter, but workarounds are available. See the [Catalog Support section](/integrations/dbt/features-and-configurations#catalog-support) for details.

This adapter is still not available for use inside [dbt Cloud](https://docs.getdbt.com/docs/dbt-cloud/cloud-overview), but we expect to make it available soon. Please reach out to support to get more information on this.

## Contributing

We welcome contributions from the community to help improve the dbt-ClickHouse adapter. Whether you’re fixing a bug,
adding a new feature, or enhancing documentation, your efforts are greatly appreciated!

Please take a moment to read our [Contribution Guide](CONTRIBUTING.md) to get started. The guide provides detailed
instructions on setting up your environment, running tests, and submitting pull requests.

## Original Author

ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their
valuable contributions.

## Run ClickHouse queries from Slack

This repository includes a lightweight slash-command service for running read-only ClickHouse queries from Slack:
`slack_clickhouse_service.py`.

### 1) Create a Slack app + slash command

1. In Slack API, create an app (or reuse an existing one).
2. Add a Slash Command (for example `/ch`).
3. Set the Request URL to your deployed service endpoint, for example:
   `https://your-domain.example/slack/commands`
4. Install the app to your workspace.
5. Copy the app **Signing Secret**.

### 2) Configure environment variables

```bash
export SLACK_SIGNING_SECRET="..."
export CLICKHOUSE_HOST="your-clickhouse-host"
export CLICKHOUSE_PORT="8443"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="..."
export CLICKHOUSE_DATABASE="default"
export CLICKHOUSE_SECURE="true"
export CLICKHOUSE_VERIFY="true"
export SLACK_MAX_ROWS="50"
export SLACK_MAX_EXECUTION_SECONDS="30"
```

Optional:

- `SLACK_COMMAND_PATH` (default: `/slack/commands`)
- `SLACK_ALLOW_UNSAFE_SQL` (default: `false`; keep this disabled unless you fully trust the command users)

### 3) Start the service

```bash
python slack_clickhouse_service.py --host 0.0.0.0 --port 3000
```

Health check:

```bash
curl http://localhost:3000/health
```

### 4) Use from Slack

In Slack:

```text
/ch SELECT now() AS current_time
```

The command responds immediately with an acknowledgement, then posts query results when complete.

### Safety defaults

- Rejects invalid Slack signatures.
- Rejects multiple SQL statements.
- Allows only read-only SQL prefixes by default (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`).
- Applies row and execution limits before sending results back to Slack.
