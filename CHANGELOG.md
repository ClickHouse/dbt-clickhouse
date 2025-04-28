### Release [1.9.0], 2025-04-28

#### New Features
* Added ability to set [SQL Security](https://clickhouse.com/docs/en/sql-reference/statements/create/view#sql_security) for normal views ([#379](https://github.com/ClickHouse/dbt-clickhouse/pull/379)).
* Add support for "microbatch" incremental strategy ([#404](https://github.com/ClickHouse/dbt-clickhouse/pull/404))
* Added support for [TTL (time-to-live)](https://clickhouse.com/docs/guides/developer/ttl) as a column configuration for `table` and `ephemeral` materializations. This feature is implemented as a [custom constraint](https://docs.getdbt.com/reference/resource-properties/constraints#custom-constraints), which requires model contracts to be enforced ([#442](https://github.com/ClickHouse/dbt-clickhouse/pull/442))
For example:

  ```sql
  -- test_ttl.sql
  {{ config(order_by='(ts)', engine='MergeTree()', materialized='table') }}

  SELECT now() AS ts, 
        'Some value that should expire!' AS col_ttl
  ```

  ```yaml
  models:
    - name: test_ttl
      description: 'Testing column-level TTL'
      config:
        contract:
          enforced: true
      columns:
        - name: ts
          data_type: timestamp
        - name: col_ttl
          data_type: String
          ttl: ts + INTERVAL 1 DAY
  ```

### Improvements
* Upgrade `dbt-core` to version `1.9` and `dbt-adapters` to `>=1.10` ([#403](https://github.com/ClickHouse/dbt-clickhouse/pull/403)).
* Escape column names in comments ([#428](https://github.com/ClickHouse/dbt-clickhouse/pull/428)).
* TTl is now available for distributed tables ([#430](https://github.com/ClickHouse/dbt-clickhouse/pull/430)).
* Previously, delete_insert would fall back to legacy silently. Now it raises an error if LWD is not enabled ([#447](https://github.com/ClickHouse/dbt-clickhouse/pull/447)).
* Refactored the logic for applying the `ON CLUSTER` clause during model creation ([#447](https://github.com/ClickHouse/dbt-clickhouse/pull/447)).
    - Previously, `ON CLUSTER` was inconsistently applied based on engine type and materialization, often leading to confusion and unexpected results.
    - Now, if a `cluster` is defined in the profile and the engine is **not** `Replicated`, the `ON CLUSTER` clause will **always** be added by default.
    - A new model-level config `disable_on_cluster: true` has been introduced to explicitly opt out of this behavior.
    - ⚠️ **Breaking Change**: This modifies the previous behavior. Users relying on the old implicit logic should review and update their models accordingly.


### Release [1.8.9], 2025-02-16

#### Improvements
* It is now possible to configure a TLS client certificate using `client_cert` and `client_cert_key` profile parameters. ([#413](https://github.com/ClickHouse/dbt-clickhouse/pull/413))
* Added Support of insert_overwrite in cluster setup with incremental and distributed_incremental materializations ([#394](https://github.com/ClickHouse/dbt-clickhouse/pull/394))
* Improve index and projections creation process ([#421](https://github.com/ClickHouse/dbt-clickhouse/pull/421)) 

#### Bugs
* Reverted breaking changes in MV materialization ([#416](https://github.com/ClickHouse/dbt-clickhouse/pull/416))
* A fix was introduced for distributed tables, where an incremental local table could have been dropped if the distributed table was missing. ([#363](https://github.com/ClickHouse/dbt-clickhouse/pull/363))


### Release [1.8.8], 2025-02-05
### Improvements
* Materialized view now attempts to use `ALTER TABLE...MODIFY QUERY` to update existing materialized views. This is an atomic operation so data is not lost. ([#390](https://github.com/ClickHouse/dbt-clickhouse/pull/390))
* Make view materialization updates atomic. ([#412](https://github.com/ClickHouse/dbt-clickhouse/pull/412))
* Create a black list settings to ignore based on the configured Engine. ([#367](https://github.com/ClickHouse/dbt-clickhouse/pull/367))

#### New Features
* [ClickHouse indexes](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes) are now fully supported for `table` materialization.


#### New Features
* [ClickHouse indexes](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes) are now fully supported for `table` materialization.
The index config should be added to the model config. for instance: 
  ```python
  {{ config(
         materialized='%s',
         indexes=[{
            'name': 'your_index_name',
            'definition': 'your_column TYPE minmax GRANULARITY 2'
         }]
  ) }}
  ```

### Bug Fixes
* Materializations are now compatible with `Replicated` database engine, as they will no longer use `ON CLUSTER` statements.

### Release [1.8.7], 2025-01-05

### New Features
* Added support for [refreshable materialized view](https://clickhouse.com/docs/en/materialized-view/refreshable-materialized-view) ([#401](https://github.com/ClickHouse/dbt-clickhouse/pull/401))

### Improvement
* Avoid potential data loss by using `CREATE OR REPLACE DICTIONARY` to atomically update a dictionary ([#393](https://github.com/ClickHouse/dbt-clickhouse/pull/393))
* Removed support in python 3.8 as it is no longer supported by dbt ([#402](https://github.com/ClickHouse/dbt-clickhouse/pull/402)

### Bug Fixes
* Fix a minor bug related to validating existence of an old hanging mv ([#396]()) 

### Release [1.8.6], 2024-12-05

### Improvement
* Today, on mv model creation, the target table is being populated with the historical data based on the query provided in the mv creation. This catchup mechanism is now behind a config flag and enabled by default (as is today). ([#399](https://github.com/ClickHouse/dbt-clickhouse/pull/399))


### Release [1.8.5], 2024-11-19

### New Features
* Added support for the creation of more than one materialized view inserting records into the same target table. ([#360](https://github.com/ClickHouse/dbt-clickhouse/pull/364))

### Improvement
* Added support for [range_hashed](https://clickhouse.com/docs/en/sql-reference/dictionaries#range_hashed) and [complex_key_range_hashed](https://clickhouse.com/docs/en/sql-reference/dictionaries#complex_key_range_hashed) layouts to the dictionary materialization. ([#361](https://github.com/ClickHouse/dbt-clickhouse/pull/361))
* Truncated stack trace for database errors for cleaner output when HIDE_STACK_TRACE variable is set to any value. ([#382](https://github.com/ClickHouse/dbt-clickhouse/pull/382))
* It is now possible to pass query settings not only on table creation but also on query. ([#362](https://github.com/ClickHouse/dbt-clickhouse/pull/362))


### Bug Fixes
* Before this version, `split_part` macro used to add an extra quotation. that was fixed in ([#338](https://github.com/ClickHouse/dbt-clickhouse/pull/338))

### Bug Fixes
* Existing local tables are no longer dropped/recreated in case of missing distributed tables in `distributed_incremental` materialization mode. ([#363](https://github.com/ClickHouse/dbt-clickhouse/pull/363))

### Release [1.8.4], 2024-09-17
### Improvement
* The S3 help macro now support a `role_arn` parameter as an alternative way to provide authentication for S3 based models.  Thanks to
[Mitchell Bregman](https://github.com/mitchbregs) for the contribution!

### Release [1.8.3], 2024-09-01
### Bug Fixes
* An [issue](https://github.com/ClickHouse/dbt-clickhouse/issues/348) was detected when using multiple projections. We solved it and added a test to cover that use case. ([#349](https://github.com/ClickHouse/dbt-clickhouse/pull/349))

### Documentation
* A [CONTRIBUTING.md](CONTRIBUTING.md) file was added to the repo. Please follow the instructions prior contributing.

### Release [1.8.2], 2024-08-22
#### New Features
* [ClickHouse projections](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection) are now fully supported for `table` materialization, and partly supported for `distributed_table` materialization.
The projection config should be added to the model config ([#342](https://github.com/ClickHouse/dbt-clickhouse/pull/342)), for instance: 
  ```python
  {{ config(
         materialized='%s',
         projections=[
             {
                 'name': 'your_projection_name',
                 'query': 'your_projection_query'
             }
         ]
  ) }}
  ```
 
#### Bug Fixes
* Until this release, when writing tests, it was not possible to pass empty seed data. ([#341](https://github.com/ClickHouse/dbt-clickhouse/pull/341))
* When a cluster was used, the adapter left a few `__dbt_backup` tables in the schema. After the fix, these backup tables are now properly dropped. ([#326](https://github.com/ClickHouse/dbt-clickhouse/pull/326))
* Due to [this GitHub change](https://github.com/actions/runner-images/issues/9692), we needed to adjust the `docker compose` command in our tests. ([#334](https://github.com/ClickHouse/dbt-clickhouse/pull/334))

#### Improvements

* Added a new **experimental** incremental strategy, `insert_overwrite`, which replaces existing data in a target table partition by partition. This ensures that only the specified partitions are overwritten, helping to maintain performance and data consistency. Note that this feature has not been tested with distributed models and may not work with such materializations.
[Anton Bryzgalov](https://github.com/bryzgaloff) Thank you for your contribution! ([#201](https://github.com/ClickHouse/dbt-clickhouse/pull/201))
* Schema changes for incremental models were improved and now include a `sync_all_column` option.
[Can Bekleyici](https://github.com/canbekley) huge thanks for your contribution. ([#332](https://github.com/ClickHouse/dbt-clickhouse/pull/332))
* Previously, view settings were only applied during view creation, not when querying the view. Starting with this release, the settings attribute will now also be applied when running queries on a view. ([#324](https://github.com/ClickHouse/dbt-clickhouse/pull/324))
* Enhanced the clickhouse__listagg macro to support single field ordering with optional direction, ensuring compatibility with ClickHouse's sorting limitations. Added validation to prevent the use of multiple order-by fields. ([#318](https://github.com/ClickHouse/dbt-clickhouse/pull/318))

#### Documentation
* Update the docs with the new `insert_overwrite` incremental strategy. ([#331](https://github.com/ClickHouse/dbt-clickhouse/pull/331)) 
* Add documentation for codec column configuration. ([#317](https://github.com/ClickHouse/dbt-clickhouse/pull/317))
 

### Release [1.8.1], 2024-07-11
#### Bug Fix
* Refresh materialized_view table only if `--full-refresh` is specified.
* Fix temporary table creation to support dbt unit tests.
* ClickHouse dictionaries are now correctly created "on cluster" when a cluster is defined.
#### Improvements
* Added database prefix option for local tables of distributed tables (until this change, only a suffix was supported).
* You can now customize tcp_keepalive configuration for native connections.
* Implement listagg ([groupArray](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/grouparray)) macro.

### Release [1.8.0], 2024-06-13
#### Improvements
- Upgrade the connector to use dbt-core 1.8.0. More info about this upgrade can be found [here](https://github.com/dbt-labs/dbt-adapters/discussions/87).

Beginning in v1.8, dbt-core and adapters are decoupled. Going forward, your installations should explicitly include both dbt-core and the desired adapter. The new pip installation command should look like this:

```pip install dbt-core dbt-clickhouse```


### Release [1.7.7], 2024-05-31
#### Bug Fix
- Fix bool_or behavior (a cross-database dbt macro ).
- Fix query_settings for models with contracts.
- Fix the option to use Nullable and LowCardinality in column constraints.
- Specifying an empty or null value for a connection_overrides field produces invalid DDL for the dictionary materialization. A fix was introduced in this release.

#### Improvements
- The connector is now supporting [column codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema#specialized-codecs).

### Release [1.7.6], 2024-04-12
#### Bug Fix
- A bug in (experimental) Distributed Table model creation could lead to errors when there was a change in the model definition (see, e.g.,
https://github.com/ClickHouse/dbt-clickhouse/issues/226).  Thanks to [Thomas Schmidt](https://github.com/Somtom) for the Fix!
- A comment at the end of a model would break the query used to retrieve the result column datatypes.  Thanks to [triou](https://github.com/tevariou)
for the bug report and the fix.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/256

#### Improvements
- The new materialization for ClickHouse dictionaries now takes an optional "credentials dictionary" argument that overrides the
global credentials values for user, password, database, host, and port (including removing any of those values by adding empty values if not needed).
This allows better control over creating dictionaries on different server.  Thanks to [Cristhian Garcia](https://github.com/Ian2012)
for the PR!
- A new `ttl` setting has been added to model configuration that will insert the provided ClickHouse TTL expression in the appropriate place.
Thanks to [Evan Rusackas](https://github.com/rusackas) for the contribution!
- The Agate library should now be lazy loaded.  This should modestly improve dbt startup times (after dbt-clickhouse is upgraded to dbt 1.8.x).
Thanks to [Daniel Reeves](https://github.com/dwreeves) for PR.

### Release [1.7.5], 2024-04-02
#### Bug Fixes
- Requirements and tests upgraded to include Python 3.12.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/264
- Model settings were not working correctly for custom materializations.  Thanks to original dbt-clickhouse [silentsokolov](https://github.com/silentsokolov)
for the PR!

### Release [1.7.4], 2024-03-23
#### Improvement
- Adds support for materializing ClickHouse dictionaries.  Thanks to [Rory Sawyer](https://github.com/SoryRawyer) for the contribution!
See his excellent [tests](https://github.com/ClickHouse/dbt-clickhouse/blob/main/tests/integration/adapter/dictionary/test_dictionary.py) 
for example usage.

### Release [1.7.3], 2024-03-11
#### Bug Fixes
- Fixed an [issue](https://github.com/ClickHouse/dbt-clickhouse/issues/231) where passing settings to on view creation didn't work.
- The `dbt test` command with a LIMIT clause were broken due to parsing error when having settings in the query ([issue](https://github.com/ClickHouse/dbt-clickhouse/issues/223)).
We added a dedicated limit placer, that takes into account the settings section (using a comment flag `-- end_of_sql` within the query).

### Release [1.7.2], 2024-02-09
#### Bug Fix
- Fixed an issue where Materialize Views would break with a custom schema.  Thanks to [Rory Sawyer](https://github.com/SoryRawyer)
for the PR!

### Release [1.7.1], 2023-12-13
#### Bug Fixes
- Some models with LIMIT clauses were broken in recent releases.  This has been fixed.  Thanks to
[ptemarvelde](https://github.com/ptemarvelde) for the PR!
- It was possible for incremental models with the delete+insert strategy to fail if ClickHouse "light weight deletes" were
not enabled or the required setting `allow_nondetermistic_mutations` was not enabled and the user did not have permission
to apply it.  This condition is now detected on startup, and an exception will be thrown if `use_lw_deletes` is configured
in the profile.  Otherwise, a warning will be logged that incremental models will be slower (because such models will
be downgraded to use the `legacy` incremental strategy).  This should prevent the confusing behavior in
https://github.com/ClickHouse/dbt-clickhouse/issues/197 by throwing an early exception for an unsupported configuration.

### Release [1.7.0], 2023-12-07
#### Improvements
- Minimal compatibility with dbt 1.7.x.  The date_spine macro and additional automated tests have not been implemented,
but are planned for a future patch release.
- DBT 1.7 introduces a (complex) optimization mechanism for retrieving a dbt catalog which is overkill for ClickHouse
(which has no separate schema/database level), so this release includes some internal catalog changes to simplify that process.

### Release [1.6.2], 2023-12-06
#### Bug Fix
- The dbt `on_schema_change` configuration value for incremental models was effectively being ignored.  This has been fixed
with a very limited implementation.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/199.  Because of the way that
ORDER BY/SORT BY/PARTITION BY/PRIMARY KEYS work in ClickHouse, plus the complexities of correctly transforming ClickHouse data types,
`sync_all_columns` is not currently supported (although an implementation that works for non-key columns is theoretically possible,
such an enhancement is not currently planned).  Accordingly, only `ignore`, `fail`, and `append_new_columns` values are supported
for `on_schema_change`.  It is also not currently supported for Distributed tables.

Note that actually appending new columns requires a fallback to the `legacy` incremental strategy, which is quite inefficient,
so while theoretically possible, using `append_new_columns` is not recommended except for very small data volumes.

### Release [1.6.1], 2023-12-04
#### Bug Fixes
- Identifier quoting was disabled for tables/databases etc.  This would cause failures for schemas or tables using reserved words
or containing special characters.  This has been fixed and some macros have been updated to correctly handle such identifiers.
Note that there still may be untested edge cases where nonstandard identifiers cause issues, so they are still not recommended.
Closes https://github.com/ClickHouse/dbt-clickhouse/issues/144. Thanks to [Alexandru Pisarenco](https://github.com/apisarenco) for the
report and initial PR!
- The new `allow_automatic_deduplication` setting was not being correctly propagated to the adapter, so setting it to `True`
did not have the intended affect.  In addition, this setting is now ignored for older ClickHouse versions that
do not support `CREATE TABLE AS SELECT ... EMPTY`, since the automatic deduplication window is required to allow correct
inserts in Replicated tables on those older versions.  Fixes https://github.com/ClickHouse/dbt-clickhouse/issues/216.

### Release [1.6.0], 2023-11-30
#### Improvements 
- Compatible with dbt 1.6.x.  Note that dbt new `clone` feature is not supported, as ClickHouse has no native "light weight"
clone functionality, and copying tables without actual data transfer is not possible in ClickHouse (barring file manipulation
outside ClickHouse itself).
- A new ClickHouse specific Materialized View materialization contributed by [Rory Sawyer](https://github.com/SoryRawyer).
This creates a ClickHouse Materialized view using the `TO` form with the name `<model_name>_mv` and the associated target
table `<model_name>`.  It's highly recommended to fully understand how ClickHouse materialized views work before using
this materialization.

### Release [1.5.2], 2023-11-28
#### Bug Fixes
- The `ON CLUSTER` clause was in the incorrect place for legacy incremental materializations.  This has been fixed.  Thanks to
[Steven Reitsma](https://github.com/StevenReitsma) for the fix!
- The `ON CLUSTER` DDL for drop tables did not include a SYNC modifier, which might be the cause of some "table already exists"
errors.  The `SYNC` modifier has been added to the `on_cluster` macro when dropping relations.
- Fixed a bug where using table settings such as `allow_nullable_key` would break "legacy" incremental materializations.  Closes
https://github.com/ClickHouse/dbt-clickhouse/issues/209.  Also see the new model `config` property `insert_settings` described
below.
- Fixed an issue where incremental materializations would incorrectly exclude duplicated inserted elements due to "automatic"
ClickHouse deduplication on replicated tables.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/213.  The fix consists
of always sending a `replicated_deduplication_window=0` table setting when creating the incremental relations.  This
behavior can be overridden by setting the new profile parameter `allow_automatic_deduplication` to `True`, although for
general dbt operations this is probably not necessary and not recommended.  Finally thanks to Andy(https://github.com/andy-miracl)
for the report and debugging help!

#### Improvements
- Added a new profile property `allow_automatic_deduplication`, which defaults to `False`.  ClickHouse Replicated deduplication is
now disable for incremental inserts, but this property can be set to true if for some reason the default ClickHouse behavior
for inserted blocks is desired.
- Added a new model `config` property `query_settings` for any ClickHouse settings that should be sent with the `INSERT INTO`
or `DELETE_FROM` queries used with materializations.  Note this is distinct from the existing property `settings` which is
used for ClickHouse "table" settings in DDL statements like `CREATE TABLE ... AS`.

### Release [1.5.1], 2023-11-27
#### Bug Fix
- Fix table materialization for compatibility with SQLFluff.  Thanks to [Kristof Szaloki](https://github.com/kris947) for the PR!

### Release [1.5.0], 2023-11-23
#### Improvements
- Compatible with dbt 1.5.x
- Contract support (using exact column data types)

#### Bug Fix
- Fix s3 macro when bucket includes `https://` prefix.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/192.

### Release [1.4.9], 2023-10-27
#### Improvement
- Lots of work on Distributed table materializations.  Big thanks to [gfunc](https://github.com/gfunc) for the additional PR
and [Zhenbang](https://github.com/zli06160) for code review and suggestions.  See the README for details on how to
use the new functionality.
#### Bug Fix
- dbt would fail if a cluster name contained a dash.  This has been fixed.  Thanks to [Andy](https://github.com/the4thamigo-uk
for the PR

### Release [1.4.8], 2023-08-22
#### Bug Fix
- Fixed issues with experimental Distributed table materializations.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/179.
Thanks to [Zhebnang](https://github.com/zli06160) for the report and for contributing to the fix with [gfunc](https://github.com/gfunc).

### Release [1.4.7], 2023-08-09
#### Bug Fix
- Fixed an exception in "legacy" incremental materializations that are not distributed

### Release [1.4.6], 2023-07-27
#### Bug fix
- Lightweight deletes could fail in environments where the HTTP session was not preserved (such as clusters behind a non-sticky
load balancer).  This has been fixed by sending the required settings with every request instead of relying on a SET statement.
A similar approach has been used to persist the 'insert_distributed_sync' setting for Distributed table materializations.

### Release [1.4.5], 2023-07-27
#### Improvement
- Adds additional experimental support for Distributed table engine models and incremental materialization.  See the README for
details.  Thanks to [gladkikhtutu](https://github.com/gladkikhtutu) for the contribution!

### Release [1.4.4], 2023-07-19
#### Bug Fixes
- Fixed two logging/exception handling issues that would cause exception on startup or when handling some exceptions
from the ClickHouse server.  Partially addresses https://github.com/ClickHouse/dbt-clickhouse/issues/169.
- Fixed issue with the `on_cluster` macro that would break the exchange tables step of incremental materializations
with an active cluster.  Thanks to [Andrew Davis](https://github.com/Savid) for the PR.  Closes
https://github.com/ClickHouse/dbt-clickhouse/issues/167

### Release [1.4.3], 2023-06-27
#### Bug Fix
- Use correct return value for `execute`.  This would cause an exception when running hooks.  Thanks to
[Sergey Reshetnikov](https://github.com/PrVrSs) for the PR.  Closed https://github.com/ClickHouse/dbt-clickhouse/issues/161

#### Improvement
- Added macros for creating distributed tables.  See the `distributed_table.sql` include file.  Thanks to
[gladkikhtutu](https://github.com/gladkikhtutu) for the contribution.  

### Release [1.4.2], 2023-05-14
#### Bug fixes
- Create initial dbt database (if not found) on the defined cluster on first run, instead of just the execution node.
Thanks to [Jens Hoevenaars](https://github.com/codenation-nl) for the PR
- Fix the SYSTEM SYNC REPLICA statement when exchanging tables ON CLUSTER for incremental materializations.  Thanks to
[Schum](https://github.com/Schum-io) for PR.  Closed https://github.com/ClickHouse/dbt-clickhouse/issues/157.

### Release [1.4.1], 2023-05-11
#### Improvements
- Reduce the number of SQL calls for Modify Comment operations.  Thanks to [Konstantin Ilchenko](https://github.com/simpl1g).
- Add "on cluster" to several additional macros to better support distributed tables.  Thanks to [Saurabh Bikram](https://github.com/saurabhbikram)
- Add the unique invocation id to temporary "new data" used in `delete+insert` incremental materializations to allow parallel transformations on the same
table.  In general parallel transformations are risky, so this approach should only be used when transformations are explicitly limited
to non-overlapping data ranges.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/150

### Release [1.4.0], 2023-02-06
#### Improvements
- Support dbt [1.4.1] https://github.com/ClickHouse/dbt-clickhouse/issues/135
  - Adds support for Python 3.11
  - Adds additional dbt 1.4.0 tests
  - Adds support for incremental_predicates.  This only applies to `delete+insert` incremental strategy.  Note that incremental
predicates that depend on "non-deterministic" data (such as a subquery using a table that is accepting inserts) could lead to
unexpected results for ReplicatedMergeTree tables depending on the timing of the incremental materialization.  
  - Replaces deprecated Exception classes
- Setting the `use_lw_deletes` profile value to True will now attempt to enable the `allow_experimental_lightweight_delete`
setting for the dbt session (if user has such permissions on the ClickHouse server).  See https://github.com/ClickHouse/dbt-clickhouse/issues/133

#### Bug Fix
- Composite unique keys specified as a list would not work with incremental materializations.  This has been fixed.


### Release [1.3.3], 2023-01-18
#### Documentation Update
- The documentation has been updated to reflect that dbt-clickhouse does support ephemeral models, and ephemeral model tests do pass.
However, due to a [ClickHouse limitation](https://github.com/ClickHouse/ClickHouse/issues/30323), CTEs will not work directly
with INSERT statements so table models will fail if they include ephemeral models in the SELECT.  View models and other SQL 
statements using ephemeral models should work correctly.

#### Bug Fix
- Client connections would incorrectly reuse a previous session_id when initializing, causing session locked errors.  This has been fixed.  This
closes https://github.com/ClickHouse/dbt-clickhouse/issues/127.  Multiple threads should now work correctly in dbt projects,
and dbt-clickhouse automated tests now use `threads=4` (soon to be the dbt default).

### Release [1.3.2], 2022-12-23
#### Improvements
- Added *experimental* support for the `delete+insert` incremental strategy.  In most cases this strategy should be significantly faster
than the existing "legacy" custom strategy (which is still the default).  To enable `delete+insert` incremental materialization, the flag `allow_experimental_lightweight_delete`
must be enabled on your ClickHouse server.  This flag is NOT currently considered production ready, so use at your own risk.  To use this strategy
as the "default" incremental strategy, the new configuration value `use_lw_deletes` must be set to True.  Otherwise, to use it for a particular model,
set the model's `incremental_strategy` to `delete+insert`.  Important caveats about this strategy:
  - This strategy directly uses lightweight deletes on the target model/table.  It does not create a temporary or intermediate table.  Accordingly,
if there is an issue during that transformation, the materialization can not be rolled back and the model may contain inconsistent data.
  - If the incremental materialization includes schema changes, or lightweight deletes are not available, dbt-clickhouse will fall back to the much
slower "legacy" strategy.
- Allow `append` as an incremental strategy.  This is the same as using the custom model configuration value `inserts_only`.
- New s3source macro.  This simplifies the process of using the ClickHouse s3 table function in queries.  See the
[tests](https://github.com/ClickHouse/dbt-clickhouse/blob/main/tests/integration/adapter/test_s3.py) for example usage.

#### Bug Fixes
- The ON CLUSTER clause has been added to additional DDL statements including incremental models processing. 
Closes https://github.com/ClickHouse/dbt-clickhouse/issues/117 and should close https://github.com/ClickHouse/dbt-clickhouse/issues/95
for Replicated tables that use the `{uuid}` macro in the path to avoid name conflicts.  Thanks to [Saurabh Bikram](https://github.com/saurabhbikram)
- The `apply` and `revoke` grants macros now correctly work with roles as well as user.  Again thanks to [Saurabh Bikram](https://github.com/saurabhbikram)
- A compound unique_key (such as `key1, key2`) now works correctly with incremental models

### Release [1.3.1], 2022-11-17
#### Improvements
- Improve error message when atomic "EXCHANGE TABLES" operation is not supported

### Release [1.3.0], 2022-10-30
#### Improvements
- Support dbt [1.3.0]  https://github.com/ClickHouse/dbt-clickhouse/issues/105
  - Adds additional dbt 1.3.0 core tests
  - Adds array utility macros ported from dbt-utils
  - Does NOT add support for Python models (not implemented)
  - Does NOT utilize default/standard incremental materialization macros (standard strategies do not work in ClickHouse)

#### Bug Fix
- Require exact match for relations.  ClickHouse databases and tables are all case-sensitive, so all searches are now case-sensitive.  Closes https://github.com/ClickHouse/dbt-clickhouse/issues/100 and https://github.com/ClickHouse/dbt-clickhouse/issues/110

</br></br>

### Release [1.2.1], 2022-09-19
#### Improvements
- Support dbt 1.2.1  https://github.com/ClickHouse/dbt-clickhouse/issues/79
    - Grants support on models
    - Support the cross database dbt-core macros (migrated from dbt-utils)
    - Add docs tests
    - Validate Python 3.10 support
    - Implement retry logic for small set of "retryable" connection errors
- Don't close connection on release to improvement model performance
- Allow specifying database engine for schema creation
- Extend support for ClickHouse Cloud and Replicated databases

#### Bug Fix
- Validate that atomic EXCHANGE TABLES is supported on the target ClickHouse server https://github.com/ClickHouse/dbt-clickhouse/issues/94

</br></br>
### Release [1.1.8], 2022-09-02
#### Improvement
- Support use of atomic EXCHANGE TABLES when supported by ClickHouse

</br></br>
### Release [1.1.7], 2022-07-11
#### Improvement
- Support ClickHouse Arrays with Nullable value types

#### Bug Fix
- Fix brackets regex in columns)
</br></br>
### Release [1.1.6], 2022-07-04
#### Improvement
- Add support for CREATE AS SELECT in Replicated Databases

</br></br>
### Release [1.1.5], 2022-06-27
#### Bug Fixes
- Ensure database exists before connection
- Safely handle dropping working database

</br></br>
### Release [1.1.4], 2022-06-27
#### Improvement
- Allow selection of either native protocol driver (clickhouse-driver) or http driver (clickhouse-connect)

</br></br>
### Release [1.1.2], 2022-06-22
#### Rename from v 1.1.0.2

</br></br>
### Release [1.1.0.2], 2022-06-17

#### Improvements
- Support for inserting SETTINGS section to CREATE AS and INSERT INTO queries through model configuration
- Support adding custom session settings to the connection through profile credentials.
- Allow using custom databases/schemas for connection
- Support Boolean fields in CSV seeds

#### Bug Fix
- Fixed prehooks and empty fields in CSV seeds

</br></br>
### Release [1.1.0], 2022-06-02

### Release [1.1.0.1], 2022-06-09

#### Bug Fix
- Fixed prehooks and empty fields in CSV seeds

</br></br>
### Release [1.1.0], 2022-06-02

#### Improvements
- 1.1.0 dbt support
- Snapshots timestamps
- Replace temporary in-memory tables with MergeTree table - removing memory limitations over Incremental model and snapshots creation
- Moved to use clickhouse-connect driver - an officially supported HTTP driver by ClickHouse Inc.
</br></br>
### Release [1.0.4], 2022-04-02

### Improvements
- 1.0.4 dbt support
- New logger
</br></br>
### Release [1.0.1] - 2022-02-09

#### Improvement
- Support 1.0.1 dbt

#### Bug Fixes
- Skip the order columns if the engine is Distributed https://github.com/ClickHouse/dbt-clickhouse/issues/14
- Fix missing optional "as" https://github.com/ClickHouse/dbt-clickhouse/issues/32
- Fix cluster name quoted https://github.com/ClickHouse/dbt-clickhouse/issues/31
</br></br>
### Release [1.0.0], 2022-01-02

#### Add
- dbt 1.0.0 support
</br></br>
### Release [0.21.1], 2022-01-01

#### Improvements
- dbt 0.21.1 support
- Extended settings for clickhouse-driver https://github.com/ClickHouse/dbt-clickhouse/issues/27

### Bug Fixes
- Fix types in CSV seed https://github.com/ClickHouse/dbt-clickhouse/issues/24

### Release [0.21.0], 2021-11-18

#### Improvement
- Support 0.21.0 dbt

#### Bug Fixes
- Fix FixString column https://github.com/ClickHouse/dbt-clickhouse/issues/20
- Default behavior for a quoted https://github.com/ClickHouse/dbt-clickhouse/issues/21
- Fix string expand https://github.com/ClickHouse/dbt-clickhouse/issues/22
</br></br>
### Release [0.20.2], 2021-10-16

#### Improvement
- dbt 0.20.1 support

#### Improvement
- Rewrite logic incremental materializations https://github.com/ClickHouse/dbt-clickhouse/issues/12

#### Bug Fixes
- Fix dbt tests with https://github.com/ClickHouse/dbt-clickhouse/pull/18 (thx @artamoshin)
- Fix relationships test https://github.com/ClickHouse/dbt-clickhouse/issues/19
</br></br>
### Release [0.20.1], 2021-08-15

#### Improvement
- dbt 0.20.1 support
</br></br>
### Release [0.20.0], 2021-08-14

#### Improvement
- dbt 0.20.0 support
</br></br>
### Release [0.19.1.1], 2021-08-13

#### Improvement
- Add verify and secure to connection configuration

#### Bug Fix
- Fix the delete expression https://github.com/ClickHouse/dbt-clickhouse/issues/12
</br></br>
### Release [0.19.1], 2021-05-07

#### Improvement
- Add support the `ON CLUSTER` clause for main cases

#### Usage Change
- Engine now require brackets `()`

#### Bug Fix
- Fix a missing sample profile
</br></br>
### Release [0.19.0.2], 2021-04-03

#### Bug Fix
- Fix name of partition
</br></br>
### Release [0.19.0.1], 2021-03-30

#### Bug Fix
- Fix version of clickhouse-driver in setup.py
</br></br>
### Release [0.19.0], 2021-02-14
#### Initial Release

[1.3.3]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.3.2..v1.3.3
[1.3.2]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.3.1..v1.3.2
[1.3.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.3.0..v1.3.1
[1.3.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.2.1..v1.3.0
[1.2.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.2.0..v1.2.1
[1.2.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.8..v1.2.0
[1.1.8]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.7..v1.1.8
[1.1.7]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.6..v1.1.7
[1.1.6]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.5..v1.1.6
[1.1.5]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.4..v1.1.5
[1.1.4]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.2..v1.1.4
[1.1.2]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.0.1..v1.1.2
[1.1.0.2]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.1.0.1..v1.1.0.2
[1.1.0.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.0.1...v1.1.0.1
[1.1.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.0.4...v1.1.0
[1.0.4]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.0.1...v1.0.4
[1.0.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.21.1...v1.0.0
[0.21.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.21.0...v0.21.1
[0.21.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.20.2...v0.21.0
[0.20.2]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.20.1...v0.20.2
[0.20.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.20.0...v0.20.1
[0.20.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.19.1.1...v0.20.0
[0.19.1.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.19.1...v0.19.1.1
[0.19.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.19.0.2...v0.19.1
[0.19.0.2]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.19.0.1...v0.19.0.2
[0.19.0.1]: https://github.com/ClickHouse/dbt-clickhouse/compare/v0.19.0...v0.19.0.1
[0.19.0]: https://github.com/ClickHouse/dbt-clickhouse/compare/eb3020a...v0.19.0
