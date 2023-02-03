from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates


class TestIncrementalPredicates(BaseIncrementalPredicates):
    def test__incremental_predicates(self, project, ch_test_version):
        if ch_test_version.startswith('22.3'):
            return  # lightweight deletes not supported in 22.3
        super().test__incremental_predicates(project)
