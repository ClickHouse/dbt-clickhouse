import pytest
from dbt.tests.util import run_dbt

schema_upper_yml = """
version: 2
sources:
  - name: seeds
    schema: "{{ target.schema }}"
    tables:
      - name: seeds_upper
        identifier: "{{ var('seed_name', 'seeds_upper') }}"
"""


seeds_upper_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()


class TestUpperCase:
    @pytest.fixture(scope="class")
    def models(self):
        config_table_sql = """
             {{ config(order_by='(some_date, id, name)', engine='MergeTree()', materialized='table',
                        settings={'allow_nullable_key': 1}) }}

            select * from {{ source('seeds', 'seeds_upper') }}
           """
        return {
            "UPPER.sql": config_table_sql,
            "schema.yml": schema_upper_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "upper_test",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seeds_upper.csv": seeds_upper_csv,
        }

    def test_upper(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt()
        assert results[0].node.search_name == 'UPPER'

        results = run_dbt()
        assert results[0].node.search_name == 'UPPER'
