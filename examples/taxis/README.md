# Taxis Large Incremental Model Example/Test

This is an example designed to test large incremental materializations.  It builds a `taxis_inc` model in the
`taxis_dbt` database that uses randomized keys to increase on each subsequent run.

## Create the source data

Use this SQL to create and populate the "source" data from the ClickHouse taxis example dataset.

```sql

CREATE DATABASE taxis;

CREATE TABLE taxis.trips (
    trip_id             UInt32,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    pickup_longitude    Nullable(Float64),
    pickup_latitude     Nullable(Float64),
    dropoff_longitude   Nullable(Float64),
    dropoff_latitude    Nullable(Float64),
    passenger_count     UInt8,
    trip_distance       Float32,
    fare_amount         Float32,
    extra               Float32,
    tip_amount          Float32,
    tolls_amount        Float32,
    total_amount        Float32,
    payment_type        LowCardinality(String),
    pickup_ntaname      LowCardinality(String),
    dropoff_ntaname     LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY trip_id;

SET input_format_skip_unknown_fields = 1;

INSERT INTO taxis.trips
SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    tip_amount,
    tolls_amount,
    total_amount,
    payment_type,
    pickup_ntaname,
    dropoff_ntaname
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..10}.gz',
    'TabSeparatedWithNames'
);
```

## Create a dbt profile entry

Use the following profile to create the associated dbt profile in the dbt_profiles.yml in ~/.dbt

```yml
taxis:
  outputs:
    dev:
      type: clickhouse
      threads: 4
      host: localhost
      port: 8123
      user: dbt_test
      password: dbt_password
      use_lw_deletes: true
      schema: taxis_dbt

  target: dev
```

## Run the model

`dbt run` in this directory should execute the model. Each run will create a somewhat larger dataset (by adding
additional random trip_ids).
