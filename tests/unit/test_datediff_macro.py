import pytest


def _render(macros, first_date, second_date, datepart):
    return ' '.join(macros.call('clickhouse__datediff', first_date, second_date, datepart).split())


class TestDatediff:
    """`dbt.datediff(first_date, second_date, datepart)` dispatches to `clickhouse__datediff`."""

    def test_expressions_are_passed_through(self, macros):
        rendered = _render(macros, 'first_date', 'toDate(second_date)', 'day')
        assert rendered == 'dateDiff( day, first_date, toDate(second_date) )'

    @pytest.mark.parametrize(
        'literal',
        ["'2016-03-09'", "'2016-03-09 10:11:12.123456'", " '2016-03-09' "],
    )
    def test_string_literal_is_read_as_datetime64(self, macros, literal):
        """dateDiff rejects a String argument."""
        rendered = _render(macros, literal, 'created_at', 'month')
        assert rendered == f'dateDiff( month, toDateTime64({literal.strip()}, 6), created_at )'

    def test_both_arguments_string_literals(self, macros):
        rendered = _render(macros, "'2023-09-01'", "'2023-09-10'", 'day')
        assert rendered == (
            "dateDiff( day, toDateTime64('2023-09-01', 6), toDateTime64('2023-09-10', 6) )"
        )

    def test_typed_literal_is_passed_through(self, macros):
        rendered = _render(macros, "'2023-09-01'::Date", "toDate('2023-09-10')", 'day')
        assert rendered == "dateDiff( day, '2023-09-01'::Date, toDate('2023-09-10') )"
