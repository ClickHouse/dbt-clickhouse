from dbt.tests.adapter.query_comment.test_query_comment import BaseQueryComments, BaseMacroQueryComments, \
    BaseMacroArgsQueryComments, BaseMacroInvalidQueryComments, BaseNullQueryComments, BaseEmptyQueryComments


class TestQueryComments(BaseQueryComments):
    pass


class TestMacroQueryComments(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryComments(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryComments(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryComments(BaseNullQueryComments):
    pass


class TestEmptyQueryComments(BaseEmptyQueryComments):
    pass
