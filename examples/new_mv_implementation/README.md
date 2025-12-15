# Streaming Materialized View Example

This example demonstrates the `external_target` feature for ClickHouse materialized views in dbt.

## Overview

In ClickHouse, Materialized Views (MVs) act as triggers that transform data and write it to a target table. The standard `materialized_view` materialization in dbt-clickhouse creates both the MV and its target table automatically.

However, there are scenarios where you want the MV to write to an **existing table** that is managed separately:

- **Fan-in pattern**: Multiple MVs writing to the same aggregation table
- **Streaming pipelines**: MVs that read from Kafka/Null tables and write to persistent storage
- **Separation of concerns**: Target table configuration (engine, TTL, etc.) managed independently from MV logic

## The `external_target` Config

When you set `external_target` in your materialized_view config, dbt will:

1. **NOT** create a target table for the MV
2. Create only the MV itself, with `TO <external_target>` clause
3. Establish a dbt dependency when using `ref()`, ensuring correct execution order

## Example Models

### 1. Target Table (`events_aggregated.sql`)

```sql
{{
  config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(event_date, event_type)'
  )
}}

-- Creates an EMPTY table with the correct schema
-- All data comes through the streaming MVs
SELECT
    toDate(now()) AS event_date,
    '' AS event_type,
    toUInt64(0) AS event_count,
    toFloat64(0) AS total_value
WHERE 0  -- No data, just schema definition
```

### 2. Streaming MV (`streaming_events_mv.sql`)

```sql
-- depends_on: {{ ref('events_aggregated') }}
{{
  config(
    materialized='materialized_view',
    external_target='events_aggregated'  -- Table name for TO clause
  )
}}

SELECT
    toDate(event_time) AS event_date,
    event_type,
    count() AS event_count,
    sum(value) AS total_value
FROM {{ source('raw_events', 'events') }}
GROUP BY event_date, event_type
```

**Note**: The `-- depends_on: {{ ref('...') }}` comment establishes the dbt DAG dependency, ensuring the target table is created first. The `external_target` config is a string with the table name.

## DAG Visualization

```
                    ┌─────────────────────┐
                    │   raw_events.events │ (source)
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
    ┌─────────────────┐  ┌───────────┐  ┌─────────────────┐
    │ events_aggregated│  │  (other   │  │ streaming_events│
    │    (table)      │◄─┤  models)  │  │      _mv        │
    └─────────────────┘  └───────────┘  └────────┬────────┘
              ▲                                   │
              │                                   │
              └───────────── writes to ───────────┘
```

## Key Points

1. **Dependency Management**: Use `-- depends_on: {{ ref('table_name') }}` comment to create the dbt DAG dependency
2. **External Target Config**: Pass the table name as a string to `external_target` (schema is auto-prefixed if not qualified)
3. **Schema Compatibility**: The MV's SELECT must produce columns compatible with the target table
4. **Independent Lifecycles**: You can `--full-refresh` the table or the MV independently
5. **Empty Target Table**: The target table is created with schema only (`WHERE 0`); all data flows through the MVs

## Running the Example

```bash
# Create the source table first (manual step)
clickhouse-client --query "CREATE TABLE IF NOT EXISTS events (
    event_id UUID,
    event_time DateTime,
    event_type String,
    user_id UInt64,
    value Float64
) ENGINE = MergeTree() ORDER BY (event_time, event_id)"

# Run dbt
dbt run

# The DAG ensures events_aggregated is created before streaming_events_mv
```

