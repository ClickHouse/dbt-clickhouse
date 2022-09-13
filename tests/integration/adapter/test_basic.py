import pytest
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.files import model_base, model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import check_relation_types, relation_from_name, run_dbt

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
key,val1,val2
abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,
""".lstrip()


class TestBaseSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestEmpty(BaseEmpty):
    pass


class TestIncremental(BaseIncremental):
    pass


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    pass


class TestSingularTests(BaseSingularTests):
    pass


class TestGenericTests(BaseGenericTests):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestMergeTreeTabelMaterializations(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_table = """
          {{ config(order_by='(some_date, id, name)', engine='MergeTree()', materialized='table',
                     settings={'allow_nullable_key': 1}) }}
        """
        base_table_sql = config_materialized_table + model_base
        return {
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        check_relation_types(project.adapter, {"table_model": "table"})

        # base table rowcount
        relation = relation_from_name(project.adapter, "table_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10


class TestInsertsOnlyIncrementalMaterializations(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_incremental = """
          {{ config(order_by='(some_date, id, name)', inserts_only=True, materialized='incremental', unique_key='id') }}
        """
        incremental_sql = config_materialized_incremental + model_incremental
        return {
            "incremental.sql": incremental_sql,
            "schema.yml": schema_base_yml,
        }


class TestCSVSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"boolean.csv": seeds_boolean_csv, "empty.csv": seeds_empty_csv}

    def test_seed(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2


class TestBaseDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=None,
            id_type="Int32",
            text_type="String",
            time_type="DateTime",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )
