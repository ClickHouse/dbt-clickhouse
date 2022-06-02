<p align="center">
  <img src="https://raw.githubusercontent.com/ClickHouse/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

[![build](https://github.com/ClickHouse/dbt-clickhouse/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/ClickHouse/dbt-clickhouse/actions/workflows/build.yml)

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

We do not support older versions of Clickhouse. The plugin uses syntax that requires version 21 or newer.

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

| Option       | Description                                                                                                                                                                                                                                                                                           | Required?                         |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|
| engine       | The table engine (type of table) to use when creating tables                                                                                                                                                                                                                                          | Optional (default: `MergeTree()`) |
| order_by     | A tuple of column names or arbitrary expressions. This allows you to create a small sparse index that helps find data faster.                                                                                                                                                                         | Optional (default: `tuple()`)     |
| partition_by | A partition is a logical combination of records in a table by a specified criterion. The partition key can be any expression from the table columns.                                                                                                                                                  | Optional                          |
| inserts_only | This property is relevant only for incremental materialization. If set to True, incremental updates will be inserted directly to the target table without creating intermediate table. This option has the potential of significantly improve performance and avoid memory limitations on big updates | Optional                          |

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
      compression: ['lz4'] # default '' (disable)
```

### Running Tests

This adapter passes all of dbt basic tests as presented in dbt's official docs: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter#testing-your-adapter.

Note: The only feature that is not supported and not tested is Ephemeral materialization.

Tests running command: 
`pytest tests/integration`

You can customize a few test params through environment variables. In order to provide custom params you'll need to create `test.env` file under root (remember not to commit this file!) and define the following env variables inside:
1. HOST_ENV_VAR_NAME - Default=`localhost`
2. USER_ENV_VAR_NAME - your ClickHouse username. Default=`default`
3. PASSWORD_ENV_VAR_NAME - your ClickHouse password. Default=''
4. PORT_ENV_VAR_NAME - ClickHouse client port. Default=9000
5. RUN_DOCKER_ENV_VAR_NAME - Identify whether to run clickhouse-server docker image (see tests/docker-compose.yml). Default=False. Set it to True if you'd like to raise a docker image (assuming docker-compose is installed in your machine) during tests that launches a clickhouse-server. Note: If you decide to run  a docker image you should set PORT_ENV_VAR_NAME to 10900 too.

### Original Author
ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their valuable contributions.

## Update 05/31/2022
* Incremental changes of an incremental model are loaded into a MergeTree table instead of in-memory temporary table. This removed memory limitations - Clickhouse recommends that in-memory table engines should not exceed 100 million rows.
* Incremental model supports 'inserts_only' mode where incremental changes are loaded directly to the target table instead of creating a temporary table for the changes and running another insert-into command. This mode is relevant only for immutable data, and can accelerate dramatically the performance of the incremental materialization.
* Fix update and delete in snapshots. 