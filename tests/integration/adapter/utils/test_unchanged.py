from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

from tests.integration.adapter.helpers import MigratedTestArgs


class TestAnyValue(MigratedTestArgs, BaseAnyValue):
    pass


class TestBaseBoolOr(MigratedTestArgs, BaseBoolOr):
    pass


class TestCastBoolToText(MigratedTestArgs, BaseCastBoolToText):
    pass


class TestConcat(MigratedTestArgs, BaseConcat):
    pass


class TestDateTrunc(MigratedTestArgs, BaseDateTrunc):
    pass


class TestEscapeSingleQuotes(MigratedTestArgs, BaseEscapeSingleQuotesQuote):
    pass


class TestEscapeSingleQuotesBackslash(MigratedTestArgs, BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(MigratedTestArgs, BaseExcept):
    pass


class TestHash(MigratedTestArgs, BaseHash):
    pass


class TestIntersect(MigratedTestArgs, BaseIntersect):
    pass


class TestLength(MigratedTestArgs, BaseLength):
    pass


class TestPosition(MigratedTestArgs, BasePosition):
    pass


class TestRight(MigratedTestArgs, BaseRight):
    pass


class TestSafeCast(MigratedTestArgs, BaseSafeCast):
    pass


class TestStringLiteral(MigratedTestArgs, BaseStringLiteral):
    pass


class TestCurrentTimestampNaive(MigratedTestArgs, BaseCurrentTimestampNaive):
    pass


class TestArrayConstruct(MigratedTestArgs, BaseArrayConstruct):
    pass
