<p align="center">
  <img src="https://raw.githubusercontent.com/ClickHouse/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

The plugin uses syntax that requires version 22.1 or newer. We do not test older versions of Clickhouse.  We also do not currently test
Replicated tables/`ON CLUSTER` functionality.

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
So we use a simple model `schema.table`, where `schema` is the Clickhouse database. Please, don't use `default` database!

### Example Profile

Default values are in brackets:

```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: [default] # ClickHouse database for dbt models

      # optional
      driver: [http] # http or native.  If not set this will be autodetermined based on port setting
      host: [localhost] 
      port: [8123]  # If not set, defaults to 8123, 8443, 9000, 9440 depending on the secure and driver settings 
      user: [default] # User for all database operations
      password: [<empty string>] # Password for the user
      cluster: [<empty string>] If set, DDL/table operations will be executed with the `ON CLUSTER` clause using this cluster
      verify: [True] # Validate TLS certificate if using TLS/SSL
      secure: [False] # Use TLS (native protocol) or HTTPS (http protocol)
      retries: [1] # Number of times to retry a "retriable" database exception (such as a 503 'Service Unavailable' error)
      compression: [<empty string>] Use gzip compression if truthy (http), or compression type for a native connection
      connect_timeout: [10] # Timeout in seconds to establish a connection to ClickHouse
      send_receive_timeout: [300] # Timeout in seconds to receive data from the ClickHouse server
      cluster_mode: [False] # Use specific settings designed to improve operation on Replicated databases (recommended for ClickHouse Cloud)
      use_lw_deletes: [False] Use the strategy `delete+insert` as the default incremental strategy.
      check_exchange: [True] # Validate that clickhouse support the atomic EXCHANGE TABLES command.  Not needed for most ClickHouse versions
      custom_settings: [{}] # A dicitonary/mapping of custom ClickHouse settings for the connection - default is empty.
      
      # Native (clickhouse-driver) connection settings
      sync_request_timeout: [5] Timeout for server ping
      compress_block_size: [1048576] Compression block size if compression is enabled
      
```

### Model Configuration

| Option       | Description                                                                                                                                                                                                                                            | Required?                         |
|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|
| engine       | The table engine (type of table) to use when creating tables                                                                                                                                                                                           | Optional (default: `MergeTree()`) |
| order_by     | A tuple of column names or arbitrary expressions. This allows you to create a small sparse index that helps find data faster.                                                                                                                          | Optional (default: `tuple()`)     |
| partition_by | A partition is a logical combination of records in a table by a specified criterion. The partition key can be any expression from the table columns.                                                                                                   | Optional                          |
| unique_key   | A tuple of column names that uniquely identify rows.  Used with incremental models for updates.                                                                                                                                                        | Optional                          |
| inserts_only | If set to True for an incremental model, incremental updates will be inserted directly to the target table without creating intermediate table. It has been deprecated in favor of the `append` incremental `strategy`, which operates in the same way | Optional                          |
| strategy     | Incremental model update strategy of `delete+insert` or `append`.  See the following Incremental Model Strategies                                                                                                                                      |                                   |

### Incremental Model Strategies

As of version 1.3.2, dbt-clickhouse supports three incremental model strategies.

#### The Default (Legacy) Strategy  

Historically ClickHouse has had only limited support for updates and deletes, in the form of asynchronous "mutations."  To emulate expected dbt behavior,
dbt-clickhouse by default creates a new temporary table containing all unaffected (not deleted, not changed) "old" records, plus any new or updated records,
and then swaps or exchanges this temporary table with the existing incremental model relation.  This is the only strategy that preserves the original relation if something
goes wrong before the operation completes; however, since it involves a full copy of the original table, it can be quite expensive and slow to execute.

#### The Delete+Insert Strategy

ClickHouse added "lightweight deletes" as an experimental feature in version 22.8.  Lightweight deletes are significantly faster than ALTER TABLE ... DELETE
operations, because they don't require rewriting ClickHouse data parts.  The incremental strategy `delete+insert` utilizes lightweight deletes to implement
incremental materializations that perform significantly better than the "legacy" strategy.  However, there are important caveats to using this strategy:
- The setting `allow_experimental_lightweight_delete` must be enabled on your ClickHouse server or including in the `custom_settings` for your ClickHouse profile.
- As suggested by the setting name, lightweight delete functionality is still experimental and there are still known issues that must be resolved before the feature is considered production ready,
so usage should be limited to datasets that are easily recreated
- This strategy operates directly on the affected table/relation (with creating any intermediate or temporary tables), so if there is an issue during the operation, the
data in the incremental model is likely to be in an invalid state

#### The Append Strategy

This strategy replaces the `inserts_only` setting in previous versions of dbt-clickhouse.  This approach simply appends new rows to the existing relation.
As a result duplicate rows are not eliminated, and there is no temporary or intermediate table.  It is the fastest approach if duplicates are either permitted
or excluded by the incremental query filter.

### Running Tests

This adapter passes all of dbt basic tests as presented in dbt's official docs: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter#testing-your-adapter.

Note: The only feature that is not supported and not tested is Ephemeral materialization.

Tests running command: 
`pytest tests/integration`

You can customize a few test params through environment variables. In order to provide custom params you can create `test.env` file under root (remember not to commit this file!) and define the following env variables inside:
1. DBT_CH_TEST_HOST - Default=`localhost`
2. DBT_CH_TEST_USER - your ClickHouse username. Default=`default`
3. DBT_CH_TEST_PASSWORD - your ClickHouse password. Default=''
4. DBT_CH_TEST_PORT - ClickHouse client port. Default=8123 (The default is automatically changed to the correct port if DBT_CH_TEST_USE_DOCKER is enabled)
6. DBT_CH_TEST_DB_ENGINE - Database engine used to create schemas.  Defaults to '' (server default)
7. DBT_CH_TEST_USE_DOCKER - Set to True to run clickhouse-server docker image (see tests/docker-compose.yml).  Requires docker-compose. Default=False
8. DBT_CH_TEST_CH_VERSION - ClickHouse docker image to use.  Defaults to `latest`
9. DBT_CH_TEST_INCLUDE_S3 - Include S3 tests.  Default=False since these are currently dependent on a specific ClickHouse S3 bucket/test dataset
10. DBT_CH_TEST_CLUSTER_MODE - Use the profile value


### Original Author
ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their valuable contributions.
