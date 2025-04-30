<p align="center">
  <img src="https://raw.githubusercontent.com/ClickHouse/dbt-clickhouse/master/etc/chdbt.png" alt="clickhouse dbt logo" width="300"/>
</p>

# dbt-clickhouse

This plugin ports [dbt](https://getdbt.com) functionality to [Clickhouse](https://clickhouse.tech/).

The plugin uses syntax that requires ClickHouse version 22.1 or newer. We do not test older versions of Clickhouse. We
also do not currently test
Replicated tables.

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

# Usage Notes

## SET Statement Warning

In many environments, using the SET statement to persist a ClickHouse setting across all DBT queries is not reliable
and can cause unexpected failures. This is particularly true when using HTTP connections through a load balancer that
distributes queries across multiple nodes (such as ClickHouse cloud), although in some circumstances this can also
happen with native ClickHouse connections. Accordingly, we recommend configuring any required ClickHouse settings in the
"custom_settings" property of the DBT profile as a best practice, instead of relying on a prehook "SET" statement as
has been occasionally suggested.

## Database

The dbt model relation identifier `database.schema.table` is not compatible with Clickhouse because Clickhouse does not
support a `schema`.
So we use a simplified approach `schema.table`, where `schema` is the Clickhouse database. Using the `default` database
is not recommended.

## Example Profile

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
      cluster: [<empty string>] # If set, certain DDL/table operations will be executed with the `ON CLUSTER` clause using this cluster. Distributed materializations require this setting to work. See the following ClickHouse Cluster section for more details.
      verify: [True] # Validate TLS certificate if using TLS/SSL
      secure: [False] # Use TLS (native protocol) or HTTPS (http protocol)
      client_cert: [null] # Path to a TLS client certificate in .pem format
      client_cert_key: [null] # Path to the private key for the TLS client certificate
      retries: [1] # Number of times to retry a "retriable" database exception (such as a 503 'Service Unavailable' error)
      compression: [<empty string>] # Use gzip compression if truthy (http), or compression type for a native connection
      connect_timeout: [10] # Timeout in seconds to establish a connection to ClickHouse
      send_receive_timeout: [300] # Timeout in seconds to receive data from the ClickHouse server
      cluster_mode: [False] # Use specific settings designed to improve operation on Replicated databases (recommended for ClickHouse Cloud)
      use_lw_deletes: [False] # Use the strategy `delete+insert` as the default incremental strategy.
      check_exchange: [True] # Validate that clickhouse support the atomic EXCHANGE TABLES command.  (Not needed for most ClickHouse versions)
      local_suffix: [_local] # Table suffix of local tables on shards for distributed materializations.
      local_db_prefix: [<empty string>] # Database prefix of local tables on shards for distributed materializations. If empty, it uses the same database as the distributed table.
      allow_automatic_deduplication: [False] # Enable ClickHouse automatic deduplication for Replicated tables
      tcp_keepalive: [False] # Native client only, specify TCP keepalive configuration. Specify custom keepalive settings as [idle_time_sec, interval_sec, probes].
      custom_settings: [{}] # A dictionary/mapping of custom ClickHouse settings for the connection - default is empty.
      
      # Native (clickhouse-driver) connection settings
      sync_request_timeout: [5] # Timeout for server ping
      compress_block_size: [1048576] # Compression block size if compression is enabled
      
```

## Model Configuration

| Option                 | Description                                                                                                                                                                                                                                                                                                          | Default if any |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| engine                 | The table engine (type of table) to use when creating tables                                                                                                                                                                                                                                                         | `MergeTree()`  |
| order_by               | A tuple of column names or arbitrary expressions. This allows you to create a small sparse index that helps find data faster.                                                                                                                                                                                        | `tuple()`      |
| partition_by           | A partition is a logical combination of records in a table by a specified criterion. The partition key can be any expression from the table columns.                                                                                                                                                                 |                |
| sharding_key           | Sharding key determines the destination server when inserting into distributed engine table.  The sharding key can be random or as an output of a hash function                                                                                                                                                      | `rand()`)      |
| primary_key            | Like order_by, a ClickHouse primary key expression.  If not specified, ClickHouse will use the order by expression as the primary key                                                                                                                                                                                |                |
| unique_key             | A tuple of column names that uniquely identify rows.  Used with incremental models for updates.                                                                                                                                                                                                                      |                |
| inserts_only           | If set to True for an incremental model, incremental updates will be inserted directly to the target table without creating intermediate table. It has been deprecated in favor of the `append` incremental `strategy`, which operates in the same way. If `inserts_only` is set, `incremental_strategy` is ignored. |                |
| incremental_strategy   | Incremental model update strategy: `delete+insert`, `append`, `insert_overwrite`, or `microbatch`.  See the following Incremental Model Strategies                                                                                                                                                                   | `default`      |
| incremental_predicates | Additional conditions to be applied to the incremental materialization (only applied to `delete+insert` strategy                                                                                                                                                                                                     |                |
| settings               | A map/dictionary of "TABLE" settings to be used to DDL statements like 'CREATE TABLE' with this model                                                                                                                                                                                                                |                |
| query_settings         | A map/dictionary of ClickHouse user level settings to be used with `INSERT` or `DELETE` statements in conjunction with this model                                                                                                                                                                                    |                |
| ttl                    | A TTL expression to be used with the table.  The TTL expression is a string that can be used to specify the TTL for the table.                                                                                                                                                                                       |                |
| indexes                | A list of indexes to create, available only for `table` materialization. For examples look at ([#397](https://github.com/ClickHouse/dbt-clickhouse/pull/397))                                                                                                                                                        |                |
| sql_security           | Allow you to specify which ClickHouse user to use when executing the view's underlying query. [`SQL SECURITY`](https://clickhouse.com/docs/sql-reference/statements/create/view#sql_security) has two legal values: `definer`, `invoker`.                                                                                                                                                |                |
| definer                | If `sql_security` was set to `definer`, you have to specify any existing user or `CURRENT_USER` in the `definer` clause.                                                                                                                                                                                             |                |
|                        |                                                                                                                                                                                                                                                                                                                      |                |


## Microbatch Configuration

| Option             | Description                                                                                                                                                                                                                                                                                                                                | Default if any |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| event_time         | The column indicating "at what time did the row occur." Required for your microbatch model and any direct parents that should be filtered.                                                                                                                                                                                                 |                |
| begin              | The "beginning of time" for the microbatch model. This is the starting point for any initial or full-refresh builds. For example, a daily-grain microbatch model run on 2024-10-01 with begin = '2023-10-01 will process 366 batches (it's a leap year!) plus the batch for "today."                                                       |                |
| batch_size         | The granularity of your batches. Supported values are `hour`, `day`, `month`, and `year`                                                                                                                                                                                                                                                   |                |
| lookback           | Process X batches prior to the latest bookmark to capture late-arriving records.                                                                                                                                                                                                                                                           | 1              |
| concurrent_batches | Overrides dbt's auto detect for running batches concurrently (at the same time). Read more about [configuring concurrent batches](https://docs.getdbt.com/docs/build/incremental-microbatch#configure-concurrent_batches). Setting to true runs batches concurrently (in parallel). false runs batches sequentially (one after the other). |                |

## Column Configuration

> **_NOTE:_** The column configuration options below require [model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts) to be enforced.

| Option | Description                                                                                                                                                | Default if any |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| codec  | A string consisting of arguments passed to `CODEC()` in the column's DDL. For example: `codec: "Delta, ZSTD"` will be compiled as `CODEC(Delta, ZSTD)`. |    
| ttl    | A string consisting of a [TTL (time-to-live) expression](https://clickhouse.com/docs/guides/developer/ttl) that defines a TTL rule in the column's DDL. For example: `ttl: ts + INTERVAL 1 DAY` will be compiled as `TTL ts + INTERVAL 1 DAY`. |

### Example

```yaml
models:
  - name: table_column_configs
    description: 'Testing column-level configurations'
    config:
      contract:
        enforced: true
    columns:
      - name: ts
        data_type: timestamp
        codec: ZSTD
      - name: x
        data_type: UInt8
        ttl: ts + INTERVAL 1 DAY
```

## ClickHouse Cluster

The `cluster` setting in profile enables dbt-clickhouse to run against a ClickHouse cluster.

### Effective Scope

If `cluster` is set in the profile, **all models will be created with the `ON CLUSTER` clause** by default—except for those using a **Replicated** engine. This includes:

- Database creation
- View materializations
- Table and incremental materializations
- Distributed materializations

Replicated engines will **not** include the `ON CLUSTER` clause, as they are designed to manage replication internally.

To **opt out** of cluster-based creation for a specific model, add the `disable_on_cluster` config:

```sql
{{ config(
        engine='MergeTree',
        materialized='table',
        disable_on_cluster='true'
    )
}}

```

table and incremental materializations with non-replicated engine will not be affected by `cluster` setting (model would
be created on the connected node only).


### Compatibility

If a model has been created without a `cluster` setting, dbt-clickhouse will detect the situation and run all DDL/DML
without `on cluster` clause for this model.

## A Note on Model Settings

ClickHouse has several types/levels of "settings". In the model configuration above, two types of these are
configurable.  `settings` means the `SETTINGS`
clause used in `CREATE TABLE/VIEW` types of DDL statements, so this is generally settings that are specific to the
specific ClickHouse table engine. The new
`query_settings` is use to add a `SETTINGS` clause to the `INSERT` and `DELETE` queries used for model materialization (
including incremental materializations).
There are hundreds of ClickHouse settings, and it's not always clear which is a "table" setting and which is a "user"
setting (although the latter are generally
available in the `system.settings` table.)  In general the defaults are recommended, and any use of these properties
should be carefully researched and tested.

## Known Limitations

* Ephemeral models/CTEs don't work if placed before the "INSERT INTO" in a ClickHouse insert statement,
  see https://github.com/ClickHouse/ClickHouse/issues/30323. This
  should not affect most models, but care should be taken where an ephemeral model is placed in model definitions and
  other SQL statements.

## Incremental Model Strategies

As of version 1.3.2, dbt-clickhouse supports three incremental model strategies.

### The Default (Legacy) Strategy

Historically ClickHouse has had only limited support for updates and deletes, in the form of asynchronous "mutations."
To emulate expected dbt behavior,
dbt-clickhouse by default creates a new temporary table containing all unaffected (not deleted, not changed) "old"
records, plus any new or updated records,
and then swaps or exchanges this temporary table with the existing incremental model relation. This is the only strategy
that preserves the original relation if something
goes wrong before the operation completes; however, since it involves a full copy of the original table, it can be quite
expensive and slow to execute.

### The Delete+Insert Strategy

ClickHouse added "lightweight deletes" as an experimental feature in version 22.8. Lightweight deletes are significantly
faster than ALTER TABLE ... DELETE
operations, because they don't require rewriting ClickHouse data parts. The incremental strategy `delete+insert`
utilizes lightweight deletes to implement
incremental materializations that perform significantly better than the "legacy" strategy. However, there are important
caveats to using this strategy:

- Lightweight deletes must be enabled on your ClickHouse server using the setting
  `allow_experimental_lightweight_delete=1` or you
  must set `use_lw_deletes=true` in your profile (which will enable that setting for your dbt sessions)
- Lightweight deletes are now production ready, but there may be performance and other problems on ClickHouse versions
  earlier than 23.3.
- This strategy operates directly on the affected table/relation (with creating any intermediate or temporary tables),
  so if there is an issue during the operation, the
  data in the incremental model is likely to be in an invalid state
- When using lightweight deletes, dbt-clickhouse enabled the setting `allow_nondeterministic_mutations`. In some very
  rare cases using non-deterministic incremental_predicates
  this could result in a race condition for the updated/deleted items (and related log messages in the ClickHouse logs).
  To ensure consistent results the
  incremental predicates should only include sub-queries on data that will not be modified during the incremental
  materialization.

### The Microbatch Strategy (Requires dbt-core >= 1.9)

The incremental strategy `microbatch` has been a dbt-core feature since version 1.9, designed to handle large
time-series data transformations efficiently. In dbt-clickhouse, it builds on top of the existing `delete_insert`
incremental strategy by splitting the increment into predefined time-series batches based on the `event_time` and
`batch_size` model configurations.

Beyond handling large transformations, microbatch provides the ability to:
- [Reprocess failed batches](https://docs.getdbt.com/docs/build/incremental-microbatch#retry).
- Auto-detect [parallel batch execution](https://docs.getdbt.com/docs/build/parallel-batch-execution).
- Eliminate the need for complex conditional logic in [backfilling](https://docs.getdbt.com/docs/build/incremental-microbatch#backfills).

For detailed microbatch usage, refer to the [official documentation](https://docs.getdbt.com/docs/build/incremental-microbatch).

### The Append Strategy

This strategy replaces the `inserts_only` setting in previous versions of dbt-clickhouse. This approach simply appends
new rows to the existing relation.
As a result duplicate rows are not eliminated, and there is no temporary or intermediate table. It is the fastest
approach if duplicates are either permitted
in the data or excluded by the incremental query WHERE clause/filter.

### The insert_overwrite Strategy (Experimental)

> [IMPORTANT]  
> Currently, the insert_overwrite strategy is not fully functional with distributed materializations.

Performs the following steps:

1. Create a staging (temporary) table with the same structure as the incremental model relation:
   `CREATE TABLE <staging> AS <target>`.
2. Insert only new records (produced by `SELECT`) into the staging table.
3. Replace only new partitions (present in the staging table) into the target table.

This approach has the following advantages:

- It is faster than the default strategy because it doesn't copy the entire table.
- It is safer than other strategies because it doesn't modify the original table until the INSERT operation completes
  successfully: in case of intermediate failure, the original table is not modified.
- It implements "partitions immutability" data engineering best practice. Which simplifies incremental and parallel data
  processing, rollbacks, etc.

The strategy requires `partition_by` to be set in the model configuration. Ignores all other strategies-specific
parameters of the model config.

## Additional ClickHouse Macros

### Model Materialization Utility Macros

The following macros are included to facilitate creating ClickHouse specific tables and views:

- `engine_clause` -- Uses the `engine` model configuration property to assign a ClickHouse table engine. dbt-clickhouse
  uses the `MergeTree` engine by default.
- `partition_cols` -- Uses the `partition_by` model configuration property to assign a ClickHouse partition key. No
  partition key is assigned by default.
- `order_cols` -- Uses the `order_by` model configuration to assign a ClickHouse order by/sorting key. If not specified
  ClickHouse will use an empty tuple() and the table will be unsorted
- `primary_key_clause` -- Uses the `primary_key` model configuration property to assign a ClickHouse primary key. By
  default, primary key is set and ClickHouse will use the order by clause as the primary key.
- `on_cluster_clause` -- Uses the `cluster` profile property to add an `ON CLUSTER` clause to certain dbt-operations:
  distributed materializations, views creation, database creation.
- `ttl_config` -- Uses the `ttl` model configuration property to assign a ClickHouse table TTL expression. No TTL is
  assigned by default.

### s3Source Helper Macro

The `s3source` macro simplifies the process of selecting ClickHouse data directly from S3 using the ClickHouse S3 table
function. It works by
populating the S3 table function parameters from a named configuration dictionary (the name of the dictionary must end
in `s3`). The macro
first looks for the dictionary in the profile `vars`, and then in the model configuration. The dictionary can contain
any of the following
keys used to populate the parameters of the S3 table function:

| Argument Name         | Description                                                                                                                                                                                  |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| bucket                | The bucket base url, such as `https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi`. `https://` is assumed if no protocol is provided.                                         |
| path                  | The S3 path to use for the table query, such as `/trips_4.gz`.  S3 wildcards are supported.                                                                                                  |
| fmt                   | The expected ClickHouse input format (such as `TSV` or `CSVWithNames`) of the referenced S3 objects.                                                                                         |
| structure             | The column structure of the data in bucket, as a list of name/datatype pairs, such as `['id UInt32', 'date DateTime', 'value String']`  If not provided ClickHouse will infer the structure. |
| aws_access_key_id     | The S3 access key id.                                                                                                                                                                        |
| aws_secret_access_key | The S3 secret key.                                                                                                                                                                           |
| role_arn              | The ARN of a ClickhouseAccess IAM role to use to securely access the S3 objects. See this [documentation](https://clickhouse.com/docs/en/cloud/security/secure-s3) for more information.     |
| compression           | The compression method used with the S3 objects.  If not provided ClickHouse will attempt to determine compression based on the file name.                                                   |

See
the [S3 test file](https://github.com/ClickHouse/dbt-clickhouse/blob/main/tests/integration/adapter/clickhouse/test_clickhouse_s3.py)
for examples of how to use this macro.

# Contracts and Constraints

Only exact column type contracts are supported. For example, a contract with a UInt32 column type will fail if the model
returns a UInt64 or other integer type.
ClickHouse also support _only_ `CHECK` constraints on the entire table/model. Primary key, foreign key, unique, and
column level CHECK constraints are not supported.
(See ClickHouse documentation on primary/order by keys.)

# Materialized Views (Experimental)

A `materialized_view` materialization should be a `SELECT` from an existing (source) table. The adapter will create a
target table with the model name
and a ClickHouse MATERIALIZED VIEW with the name `<model_name>_mv`. Unlike PostgreSQL, a ClickHouse materialized view is
not "static" (and has
no corresponding REFRESH operation). Instead, it acts as an "insert trigger", and will insert new rows into the target
table using the defined `SELECT`
"transformation" in the view definition on rows inserted into the source table. See the [test file](https://github.com/ClickHouse/dbt-clickhouse/blob/main/tests/integration/adapter/materialized_view/test_materialized_view.py)
for an introductory example
of how to use this functionality.

Clickhouse provides the ability for more than one materialized view to write records to the same target table. To
support this in dbt-clickhouse, you can construct a `UNION` in your model file, such that the SQL for each of your
materialized views is wrapped with comments of the form `--my_mv_name:begin` and `--my_mv_name:end`.

For example the following will build two materialized views both writing data to the same destination table of the
model. The names of the materialized views will take the form `<model_name>_mv1` and `<model_name>_mv2` :

```
--mv1:begin
select a,b,c from {{ source('raw', 'table_1') }}
--mv1:end
union all
--mv2:begin
select a,b,c from {{ source('raw', 'table_2') }}
--mv2:end
```

> IMPORTANT!
>
> When updating a model with multiple materialized views (MVs), especially when renaming one of the MV names,
> dbt-clickhouse does not automatically drop the old MV. Instead,
> you will encounter the following warning:
`Warning - Table <previous table name> was detected with the same pattern as model name <your model name> but was not found in this run. In case it is a renamed mv that was previously part of this model, drop it manually (!!!) `

## Data catchup

Currently, when creating a materialized view (MV), the target table is first populated with historical data before the
MV itself is created.

In other words, dbt-clickhouse initially creates the target table and preloads it with historical data based on the
query defined for the MV. Only after this step is the MV created.

If you prefer not to preload historical data during MV creation, you can disable this behavior by setting the catchup
config to False:

```python
{{config(
    materialized='materialized_view',
    engine='MergeTree()',
    order_by='(id)',
    catchup=False
)}}
```

## Refreshable Materialized Views

To use [Refreshable Materialized View](https://clickhouse.com/docs/en/materialized-view/refreshable-materialized-view),
please adjust the following configs as needed in your MV model (all these configs are supposed to be set inside a
refreshable config object):

| Option                | Description                                                                                                                                                              | Required | Default Value |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| refresh_interval      | The interval clause (required)                                                                                                                                           | Yes      |               |
| randomize             | The randomization clause, will appear after `RANDOMIZE FOR`                                                                                                              |          |               |
| append                | If set to `True`, each refresh inserts rows into the table without deleting existing rows. The insert is not atomic, just like a regular INSERT SELECT.                  |          | False         |
| depends_on            | A dependencies list for the refreshable mv. Please provide the dependencies in the following format `{schema}.{view_name}`                                               |          |               |
| depends_on_validation | Whether to validate the existence of the dependencies provided in `depends_on`. In case a dependency doesn't contain a schema, the validation occurs on schema `default` |          | False         |

A config example for refreshable materialized view:

```python
{{
    config(
        materialized='materialized_view',
        refreshable={
            "interval": "EVERY 5 MINUTE",
            "randomize": "1 MINUTE",
            "append": True,
            "depends_on": ['schema.depend_on_model'],
            "depends_on_validation": True
        }
    )
}}
```

### Limitations

* When creating a refreshable materialized view (MV) in ClickHouse that has a dependency, ClickHouse does not throw an
  error if the specified dependency does not exist at the time of creation. Instead, the refreshable MV remains in an
  inactive state, waiting for the dependency to be satisfied before it starts processing updates or refreshing.
  This behavior is by design, but it may lead to delays in data availability if the required dependency is not addressed
  promptly. Users are advised to ensure all dependencies are correctly defined and exist before creating a refreshable
  materialized view.
* As of today, there is no actual "dbt linkage" between the mv and its dependencies, therefore the creation order is not
  guaranteed.
* The refreshable feature was not tested with multiple mvs directing to the same target model.

# Dictionary materializations (experimental)

See the tests
in https://github.com/ClickHouse/dbt-clickhouse/blob/main/tests/integration/adapter/dictionary/test_dictionary.py for
examples of how to
implement materializations for ClickHouse dictionaries

# Distributed materializations

Notes:

- dbt-clickhouse queries now automatically include the setting `insert_distributed_sync = 1` in order to ensure that
  downstream incremental
  materialization operations execute correctly. This could cause some distributed table inserts to run more slowly than
  expected.

## Distributed table materialization

Distributed table created with following steps:

1. Creates temp view with sql query to get right structure
2. Create empty local tables based on view
3. Create distributed table based on local tables.
4. Data inserts into distributed table, so it is distributed across shards without duplicating.

### Distributed table model example

```sql
{{
    config(
        materialized='distributed_table',
        order_by='id, created_at',
        sharding_key='cityHash64(id)',
        engine='ReplacingMergeTree'
    )
}}

select id, created_at, item
from {{ source('db', 'table') }}
```

### Generated migrations

```sql
CREATE TABLE db.table_local on cluster cluster
(
    `id`
    UInt64,
    `created_at`
    DateTime,
    `item`
    String
)
    ENGINE = ReplacingMergeTree
    ORDER BY
(
    id,
    created_at
)
    SETTINGS index_granularity = 8192;


CREATE TABLE db.table on cluster cluster
(
    `id`
    UInt64,
    `created_at`
    DateTime,
    `item`
    String
)
    ENGINE = Distributed
(
    'cluster',
    'db',
    'table_local',
    cityHash64
(
    id
));
```

## Distributed incremental materialization

Incremental model based on the same idea as distributed table, the main difficulty is to process all incremental
strategies correctly.

1. _The Append Strategy_ just insert data into distributed table.
2. _The Delete+Insert_ Strategy creates distributed temp table to work with all data on every shard.
3. _The Default (Legacy) Strategy_ creates distributed temp and intermediate tables for the same reason.

Only shard tables are replacing, because distributed table does not keep data.
The distributed table reloads only when the full_refresh mode is enabled or the table structure may have changed.

### Distributed incremental model example

```sql
{{
    config(
        materialized='distributed_incremental',
        engine='MergeTree',
        incremental_strategy='append',
        unique_key='id,created_at'
    )
}}

select id, created_at, item
from {{ source('db', 'table') }}
```

### Generated migrations

```sql
CREATE TABLE db.table_local on cluster cluster
(
    `id`
    UInt64,
    `created_at`
    DateTime,
    `item`
    String
)
    ENGINE = MergeTree
    SETTINGS index_granularity = 8192;


CREATE TABLE db.table on cluster cluster
(
    `id`
    UInt64,
    `created_at`
    DateTime,
    `item`
    String
)
    ENGINE = Distributed
(
    'cluster',
    'db',
    'table_local',
    cityHash64
(
    id
));
```

## Contributing

We welcome contributions from the community to help improve the dbt-ClickHouse adapter. Whether you’re fixing a bug,
adding a new feature, or enhancing documentation, your efforts are greatly appreciated!

Please take a moment to read our [Contribution Guide](CONTRIBUTING.md) to get started. The guide provides detailed
instructions on setting up your environment, running tests, and submitting pull requests.

## Original Author

ClickHouse wants to thank @[silentsokolov](https://github.com/silentsokolov) for creating this connector and for their
valuable contributions.
