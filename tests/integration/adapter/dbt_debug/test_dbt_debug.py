import re

from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug
from dbt.tests.util import run_dbt


class TestDebugClickHouse(BaseDebug):
    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_nopass(self, project):
        run_dbt(["debug", "--target", "nopass"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+profiles\.yml file"), "ERROR invalid")

    def test_wronguser(self, project):
        run_dbt(["debug", "--target", "wronguser"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+Connection test"), "ERROR")

    def test_empty_target(self, project):
        run_dbt(["debug", "--target", "none_target"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), "misconfigured")
