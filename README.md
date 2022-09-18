<p align="center">
  <img src="https://raw.githubusercontent.com/ClickHouse/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

[![build](https://github.com/ClickHouse/dbt-clickhouse/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/ClickHouse/dbt-clickhouse/actions/workflows/build.yml)

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

We do not test over older versions of Clickhouse. The plugin uses syntax that requires version 22.1 or newer.

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
- [x] Snapshots
- [ ] Ephemeral materialization

# Usage Notes

### Database

The dbt model `database.schema.table` is not compatible with Clickhouse because Clickhouse does not support a `schema`.
So we use a simple model `schema.table`, where `schema` is the Clickhouse's database. Please, don't use `default` database!

### Model Configuration

| Option       | Description                                                                                                                                                                                                                                                                                            | Required?                         |
|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|
| engine       | The table engine (type of table) to use when creating tables                                                                                                                                                                                                                                           | Optional (default: `MergeTree()`) |
| order_by     | A tuple of column names or arbitrary expressions. This allows you to create a small sparse index that helps find data faster.                                                                                                                                                                          | Optional (default: `tuple()`)     |
| partition_by | A partition is a logical combination of records in a table by a specified criterion. The partition key can be any expression from the table columns.                                                                                                                                                   | Optional                          |
| unique_key   | A tuple of column names that uniquely identify rows. For more details on uniqueness constraints, see here.                                                                                                                                                                                             | Optional                          |
| inserts_only | This property is relevant only for incremental materialization. If set to True, incremental updates will be inserted directly to the target table without creating intermediate table. This option has the potential of significantly improve performance and avoid memory limitations on big updates. | Optional                          |
| settings     | A dictionary with custom settings for INSERT INTO and CREATE AS SELECT queries.                                                                                                                                                                                                                        | Optional                          |

### Example Profile

```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: [database name]

      # optional
      driver: [http] # http or native.  If not set will autodetermine base one port
      port: [port]  # default 8123
      user: [user] # default 'default'
      host: [db.clickhouse.com] # default localhost
      password: [password] # default ''
      verify: [verify] # default True
      secure: [secure] # default False
      connect_timeout: [10] # default 10 seconds.
      custom_settings: {} # Custom seetings for the connection - default is empty.
```

### Running Tests

This adapter passes all of dbt basic tests as presented in dbt's official docs: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter#testing-your-adapter.

Note: The only feature that is not supported and not tested is Ephemeral materialization.

Tests running command: 
`pytest tests/integration`

You can customize a few test params through environment variables. In order to provide custom params you can create `test.env` file under root (remember not to commit this file!) and define the following env variables inside:
1. DBT_CH_TEST_HOST - Default=`localhost`
2. DBT_CH_TEST_USER - your ClickHouse username. Default=`default`
3. DBT_CH_TEST_PASSWORD - your ClickHouse password. Default=''
4. DBT_CH_TEST_PORT - ClickHouse client port. Default=8123
5. DBT_CH_TEST_DATABASE - Explicit database (dbt 'schema') used to execute queries for test setup.  Defaults to ClickHouse user default.  Note that each test will actually generate a new random database.
6. DBT_CH_TEST_DB_ENGINE - Database engine used to create schemas.  Defaults to '' (server default)
7. DBT_CH_TEST_USE_DOCKER - Identify whether to run clickhouse-server docker image (see tests/docker-compose.yml). Default=False. Set it to True if you'd like to raise a docker image (assuming docker-compose is installed in your machine) during tests that launches a clickhouse-server. Note: If you decide to run a docker image you should set DBT_CH_TEST_PORT to 10900 too.

### Original Author
ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their valuable contributions.
