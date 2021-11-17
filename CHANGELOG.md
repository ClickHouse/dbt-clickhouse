## [Unreleased]

## [0.20.2] - 2021-10-16

### Add
- Support 0.20.1 dbt

### Change
- Rewrite logic incremental materializations #12

### Fix
- Fix dbt tests with ClickHouse #18 (thx @artamoshin)
- Fix relationships test #19

## [0.20.1] - 2021-08-15

### Add
- Support 0.20.1 dbt

## [0.20.0] - 2021-08-14

### Add
- Support 0.20.0 dbt

## [0.19.1.1] - 2021-08-13

### Add
- Add verify and secure to connection configuration

### Fix
- Fix the delete expression #12

## [0.19.1] - 2021-05-07

### Add
- Add support the `ON CLUSTER` clause for main cases

### Change
- Engine now require brackets `()`

### Fix
- Fix a missing sample profile

## [0.19.0.2] - 2021-04-03

### Fix
- Fix name of partition

## [0.19.0.1] - 2021-03-30

### Fix
- Fix version clickhouse-driver in setup.py

## [0.19.0] - 2021-02-14

Init relaase

[Unreleased]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.20.2...HEAD
[0.20.2]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.20.1...v0.20.2
[0.20.1]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.20.0...v0.20.1
[0.20.0]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.19.1.1...v0.20.0
[0.19.1.1]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.19.1...v0.19.1.1
[0.19.1]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.19.0.2...v0.19.1
[0.19.0.2]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.19.0.1...v0.19.0.2
[0.19.0.1]: https://github.com/silentsokolov/dbt-clickhouse/compare/v0.19.0...v0.19.0.1
[0.19.0]: https://github.com/silentsokolov/dbt-clickhouse/compare/eb3020a...v0.19.0