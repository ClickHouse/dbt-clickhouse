import pytest
from dbt.exceptions import CompilationError
from dbt.tests.adapter.hooks.fixtures import models__hooks_error
from dbt.tests.util import run_dbt


class TestDuplicateHooksInConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_error}

    def test_run_duplicate_hook_defs(self, project):
        with pytest.raises(CompilationError) as exc:
            run_dbt()
        assert "pre_hook" in str(exc.value)
        assert "pre-hook" in str(exc.value)
