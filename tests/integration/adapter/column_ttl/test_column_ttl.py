import pytest
from dbt.tests.util import get_manifest, run_dbt
from fixtures_column_ttl import (
    column_ttl_model_yml,
    column_ttl_model_sql
)


class TestTtlColumn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "column_ttl_model.yml": column_ttl_model_yml,
            "column_ttl_model.sql": column_ttl_model_sql
        }

    def test_ttl_column(self, project):
        run_dbt(["run", "-s", "column_ttl_model"], expect_pass=True)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.column_ttl_model"
        id_column_config = manifest.nodes[model_id].columns.get("id")
        assert id_column_config._extra.get("ttl") == "toStartOfDay(datetime_col) + INTERVAL 1 DAY"


