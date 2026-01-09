"""
test UDF creation support for dbt-clickhouse
"""

# we'll import helper used by the core test project to write directories

import pytest

from dbt.tests.util import run_dbt

UDF_MODEL = """
x * 2
""".strip()

UDF_SCHEMA_YML = """
functions:
  - name: answer_to_everything
    description: Computes the answer to life, universe and everything.
    arguments:
      - name: x
        data_type: Int32
    returns:
      data_type: Int32
""".strip()


class TestUDFCreation:
    # Original functions fixture: provide files for a dedicated 'functions' dir
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "answer_to_everything.sql": UDF_MODEL,
            "answer_to_everything.yml": UDF_SCHEMA_YML,
        }

    def test_create(self, project):
        run_dbt(["build"])

        result = project.run_sql("SELECT answer_to_everything(21)", fetch="one")
        assert result[0] == 42
