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

## Contributing

We welcome contributions from the community to help improve the dbt-ClickHouse adapter. Whether you’re fixing a bug,
adding a new feature, or enhancing documentation, your efforts are greatly appreciated!

Please take a moment to read our [Contribution Guide](CONTRIBUTING.md) to get started. The guide provides detailed
instructions on setting up your environment, running tests, and submitting pull requests.

## Original Author

ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their
valuable contributions.
