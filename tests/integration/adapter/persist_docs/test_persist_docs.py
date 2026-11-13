import os

import pytest
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
)
from dbt.tests.util import run_dbt_and_capture

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsAllColumnsMissing(BasePersistDocsAllColumnsMissing):
    def test_all_columns_missing_does_not_crash(self, project):
        if not IS_CORE_V2:
            return super().test_all_columns_missing_does_not_crash(project)

        # dbt core v2 reports a model that emitted a warning as `warn`, dbt-core as `success`.
        results, logs = run_dbt_and_capture(["run"])
        assert len(results) == 1
        assert results[0].status == "warn"
        assert "bogus_col_one" in logs
        assert "bogus_col_two" in logs


class TestPersistDocsQuotedColumnCaseSensitive(BasePersistDocsQuotedColumnCaseSensitive):
    pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatch(
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch
):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    # The base fixture writes `materialized` without the `+` prefix, which dbt core v2 rejects
    # (dbt-core only deprecates it). The prefixed key works on both engines.
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialized": "table",
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }
