import pytest


class TestDate:
    """`dbt.date(year, month, day)` dispatches to `clickhouse__date`."""

    @pytest.mark.parametrize(
        'year, month, day, expected',
        [
            (2023, 9, 7, "toDate('2023-09-07')"),
            (1970, 1, 1, "toDate('1970-01-01')"),
            (2149, 6, 6, "toDate('2149-06-06')"),
        ],
    )
    def test_date_in_date_range(self, macros, year, month, day, expected):
        assert macros.call('clickhouse__date', year, month, day).strip() == expected

    @pytest.mark.parametrize(
        'year, month, day, expected',
        [
            (1969, 12, 31, "toDate32('1969-12-31')"),
            (1900, 1, 1, "toDate32('1900-01-01')"),
            (2149, 6, 7, "toDate32('2149-06-07')"),
            (2299, 12, 31, "toDate32('2299-12-31')"),
        ],
    )
    def test_date_outside_date_range_uses_date32(self, macros, year, month, day, expected):
        """`Date` clamps a value outside 1970-01-01..2149-06-06 to the nearest bound."""
        assert macros.call('clickhouse__date', year, month, day).strip() == expected
