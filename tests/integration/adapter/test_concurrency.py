from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency, seeds__update_csv
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)


class TestConcurrency(BaseConcurrency):
    def test_clickhouse_concurrency(self, project):
        run_dbt(["seed", "--select", "seed"])
        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        self._validate_results(project, results, output)

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)

        self._validate_results(project, results, output)

    def _validate_results(self, project, results, output):
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")
        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
