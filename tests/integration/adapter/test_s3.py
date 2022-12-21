import os

import pytest
from dbt.tests.util import run_dbt

testing_s3 = os.environ.get('DBT_CH_TEST_INCLUDE_S3', '').lower() in ('1', 'true', 'yes')
pytestmark = pytest.mark.skipif(not testing_s3, reason='Testing S3 disabled')

schema_yaml = """
version: 2

models:
  - name: s3_taxis
    description: NY Taxi dataset from S3
    config:
      materialized: table
      order_by: pickup_datetime
      s3_url: "https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_4.gz"
      s3_format: TabSeparatedWithNames
      s3_structure:
        - 'trip_id UInt32'
        - 'pickup_datetime DateTime'
"""


class TestBasicS3:
    @pytest.fixture(scope="class")
    def models(self):
        s3_taxis_sql = """
            select * from {{ clickhouse_s3table(format='TabSeparatedWithNames') }} LIMIT 5000
           """
        return {"s3_taxis.sql": s3_taxis_sql, "schema.yml": schema_yaml}

    def test_s3_model(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from s3_taxis", fetch="one")
        assert result[0] == 5000
