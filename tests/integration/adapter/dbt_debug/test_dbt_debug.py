import os
import re

import pytest
from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug
from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtRuntimeError

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))


class TestDebugClickHouse(BaseDebug):
    def _assert_unknown_target_v2(self, target):
        # None of these targets exist in the test profile. dbt core v2 rejects an
        # unknown --target while loading the profile, before `debug` prints its
        # per-check report, and the harness surfaces that as DbtRuntimeError.
        with pytest.raises(DbtRuntimeError) as exc:
            run_dbt(["debug", "--target", target], expect_pass=False)
        assert f"target '{target}' not found in profile" in str(exc.value)

    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_nopass(self, project):
        if IS_CORE_V2:
            self._assert_unknown_target_v2("nopass")
            return
        run_dbt(["debug", "--target", "nopass"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+profiles\.yml file"), "ERROR invalid")

    def test_wronguser(self, project):
        if IS_CORE_V2:
            self._assert_unknown_target_v2("wronguser")
            return
        run_dbt(["debug", "--target", "wronguser"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+Connection test"), "ERROR")

    def test_empty_target(self, project):
        if IS_CORE_V2:
            self._assert_unknown_target_v2("none_target")
            return
        run_dbt(["debug", "--target", "none_target"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), "misconfigured")
