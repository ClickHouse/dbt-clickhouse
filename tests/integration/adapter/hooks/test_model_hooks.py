import os

import pytest
from dbt.exceptions import CompilationError
from dbt.tests.adapter.hooks.fixtures import models__hooks_error
from dbt.tests.util import run_dbt


@pytest.mark.skipif(
    bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY')),
    reason='dbt core v2 accepts a config that sets both `pre_hook` and `pre-hook` and keeps '
    'one of them instead of failing: https://github.com/dbt-labs/dbt/issues/15985',
)
class TestDuplicateHooksInConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_error}

    def test_run_duplicate_hook_defs(self, project):
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "pre_hook" in str(exc.value)
        assert "pre-hook" in str(exc.value)
