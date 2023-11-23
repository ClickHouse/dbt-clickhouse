import os

import pytest
from dbt.tests.util import run_dbt

testing_s3 = os.environ.get('DBT_CH_TEST_INCLUDE_S3', '').lower() in ('1', 'true', 'yes')
pytestmark = pytest.mark.skipif(not testing_s3, reason='Testing S3 disabled')

schema_yaml = """
version: 2

models:
  - name: s3_taxis_source
    description: NY Taxi dataset from S3
    config:
      materialized: table
      order_by: pickup_datetime
      unique_id: trip_id
      taxi_s3:
        structure:
          - 'trip_id UInt32'
          - 'pickup_datetime DateTime'
  - name: s3_taxis_inc
"""

s3_taxis_source = """
select * from {{ clickhouse_s3source('taxi_s3', path='/trips_4.gz') }} LIMIT 5000
"""

s3_taxis_full_source = """
select * from {{ clickhouse_s3source('taxi_s3', path='/trips_5.gz') }} LIMIT 1000
"""

s3_taxis_inc = """
{{ config(
    materialized='incremental',
    order_by='pickup_datetime',
    incremental_strategy='delete+insert',
    unique_key='trip_id',
    taxi_s3={"structure":['trip_id UInt32', 'pickup_datetime DateTime', 'passenger_count UInt8']}
    )
}}

{% if is_incremental() %}
  select * from {{ clickhouse_s3source('taxi_s3', path='/trips_4.gz') }}
    where pickup_datetime > (SELECT addDays(max(pickup_datetime), -2) FROM s3_taxis_inc)
{% else %}
  select trip_id, pickup_datetime, toUInt8(0) as passenger_count from s3_taxis_source
{% endif %}
LIMIT 5000
"""


class TestS3:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'vars': {
                'taxi_s3': {
                    'bucket': 'datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/',
                    'fmt': 'TabSeparatedWithNames',
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "s3_taxis_source.sql": s3_taxis_source,
            "s3_taxis_inc.sql": s3_taxis_inc,
            "schema.yml": schema_yaml,
        }

    def test_s3_incremental(self, project):
        run_dbt(["run", "--select", "s3_taxis_source.sql"])
        result = project.run_sql("select count() as num_rows from s3_taxis_source", fetch="one")
        assert result[0] == 5000

        run_dbt(["run", "--select", "s3_taxis_inc.sql"])
        result = project.run_sql(
            "select count(), sum(passenger_count) as num_rows from s3_taxis_inc", fetch="one"
        )
        assert result == (5000, 0)

        run_dbt(["run", "--select", "s3_taxis_inc.sql"])
        result = project.run_sql(
            "select count(), sum(passenger_count) as num_rows from s3_taxis_inc", fetch="one"
        )
        assert 5000 < result[0] < 10000
        assert result[1] > 0


class TestS3Bucket:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'vars': {
                'taxi_s3': {
                    'bucket': 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/',
                    'fmt': 'TabSeparatedWithNames',
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "s3_taxis_source.sql": s3_taxis_full_source,
            "schema.yml": schema_yaml,
        }

    def test_read(self, project):
        run_dbt(["run", "--select", "s3_taxis_source.sql"])
        result = project.run_sql("select count() as num_rows from s3_taxis_source", fetch="one")
        assert result[0] == 1000
