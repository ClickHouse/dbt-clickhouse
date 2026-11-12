import os

import pytest
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))


class TestQueryComments(BaseQueryComments):
    pass


class TestMacroQueryComments(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryComments(BaseMacroArgsQueryComments):
    pass


@pytest.mark.skipif(
    IS_CORE_V2,
    reason='dbt core v2 validates the raw query-comment template only, so a macro-rendered '
    '"*/" is not rejected: https://github.com/dbt-labs/dbt/issues/16630',
)
class TestMacroInvalidQueryComments(BaseMacroInvalidQueryComments):
    pass


@pytest.mark.skipif(
    IS_CORE_V2,
    reason='dbt core v2 reads `query-comment: null` as a missing key and keeps the default '
    'comment: https://github.com/dbt-labs/dbt/issues/17166',
)
class TestNullQueryComments(BaseNullQueryComments):
    pass


class TestEmptyQueryComments(BaseEmptyQueryComments):
    pass
