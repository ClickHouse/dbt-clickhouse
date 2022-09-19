import pytest
from dbt.tests.adapter.utils.fixture_replace import models__test_replace_yml
from dbt.tests.adapter.utils.test_replace import BaseReplace

models__test_replace_sql = """
select

    {{ replace('string_text', 'a', 'b') }} as actual,
    result as expected

from {{ ref('data_replace') }} WHERE 'string_text' = 'a'

UNION ALL

select

    {{ replace('string_text', 'http://', '') }} as actual,
    result as expected

from {{ ref('data_replace') }} WHERE 'string_text' LIKE '%google%'

"""


class TestReplace(BaseReplace):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }
