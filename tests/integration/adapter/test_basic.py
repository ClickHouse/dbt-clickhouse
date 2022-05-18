import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.files import model_base, schema_base_yml
from dbt.tests.util import run_dbt, check_relation_types, relation_from_name


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


class TestEphemeralMyAdapter(BaseEphemeral):
    pass


class TestSingularTestsEphemeralMyAdapter(BaseSingularTestsEphemeral):
    pass


class TestMergeTreeTabelMaterializations(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_table = """
          {{ config(order_by='(some_date, id, name)', engine='MergeTree()', materialized='table') }}
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
