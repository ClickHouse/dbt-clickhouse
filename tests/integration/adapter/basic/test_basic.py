import os

import pytest
from dbt.tests.util import run_dbt

# CSV content with boolean column type.
seeds_boolean_csv = """
key,value
abc,true
def,false
hij,true
klm,false
""".lstrip()

# CSV content with empty fields.
seeds_empty_csv = """
key,val1,val2,str1
abc,1,1,some_str
abc,1,0,"another string"
def,1,0,
hij,1,1,Caps
hij,1,,"second string"
klm,1,0,"test"
klm,1,,"test4"
""".lstrip()

seeds_schema_yml = """
version: 2

seeds:
  - name: empty
    config:
      column_types:
        val2: Nullable(UInt32)
        str1: Nullable(String)
      settings:
        allow_nullable_key: 1
"""

replicated_seeds_schema_yml = """
version: 2

seeds:
  - name: empty
    config:
      engine: ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}' )
      column_types:
        val2: Nullable(UInt32)
        str1: Nullable(String)
"""

base_seeds_schema_yml = """
version: 2

seeds:
  - name: base
    config:
      engine: ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}' )
"""


class TestCSVSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "schema.yml": seeds_schema_yml,
            "boolean.csv": seeds_boolean_csv,
            "empty.csv": seeds_empty_csv,
        }

    def test_seed(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2
        columns = project.run_sql("DESCRIBE TABLE empty", fetch='all')
        assert columns[2][1] == 'Nullable(UInt32)'
        assert columns[3][1] == 'Nullable(String)'


class TestReplicatedCSVSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "schema.yml": replicated_seeds_schema_yml,
            "empty.csv": seeds_empty_csv,
        }

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_seed(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE empty", fetch='all')
        assert columns[2][1] == 'Nullable(UInt32)'
        assert columns[3][1] == 'Nullable(String)'
