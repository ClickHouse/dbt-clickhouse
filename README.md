<p align="center">
  <img src="https://raw.githubusercontent.com/silentsokolov/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

[![build](https://github.com/silentsokolov/dbt-clickhouse/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/silentsokolov/dbt-clickhouse/actions/workflows/build.yml)

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

We have not tested extensively against older versions of Clickhouse. The plugin uses syntax that requires version 20.11 or newer.

### Installation

Use your favorite Python package manager to install the app from PyPI, e.g.

```bash
pip install dbt-clickhouse
```

### Supported features

- [x] Table materialization
- [x] View materialization
- [x] Incremental materialization
- [x] Seeds
- [x] Sources
- [x] Docs generate
- [x] Tests
- [x] Snapshots (experimental)
- [ ] Ephemeral materialization

# Usage Notes

### Database

The dbt model `database.schema.table` is not compatible with Clickhouse because Clickhouse does not support a `schema`.
So we use a simple model `schema.table`, where `schema` is the Clickhouse's database. Please, don't use `default` database!

### Model Configuration

| Option         | Description                                                                                                                                          | Required?                         |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|
| engine         | The table engine (type of table) to use when creating tables                                                                                         | Optional (default: `MergeTree()`) |
| order_by     | A tuple of column names or arbitrary expressions. This allows you to create a small sparse index that helps find data faster.                        | Optional (default: `tuple()`)     |
| partition_by | A partition is a logical combination of records in a table by a specified criterion. The partition key can be any expression from the table columns. | Optional                          |

### Example Profile

```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: [database name]
      host: [db.clickhouse.com]

      # optional
      port: [port]  # default 9000
      user: [user]
      password: [abc123]
      cluster: [cluster name]
      verify: [verify] # default False
      secure: [secure] # default False
      connect_timeout: [10] # default 10
      send_receive_timeout: [300] # default 300
      sync_request_timeout: [5] # default 5
      compress_block_size: [1048576] # default 1048576
      compression: ['lz4'] # default '' (disable)
```
