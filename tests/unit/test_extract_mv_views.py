import pytest

MULTIPLE_MV_SQL = """
--mv1:begin
select id, name from people where department = 'engineering'
--mv1:end

union all

--mv2:begin
select id, name from people where department = 'sales'
--mv2:end
"""


class TestExtractMvViews:
    def test_no_markers_yields_single_mv_suffix(self, macros):
        """The key `mv` is a suffix on the relation of the model. The view is `<model>_mv`."""
        sql = 'select id, name from people'
        assert macros.call('clickhouse__extract_mv_views', sql) == {'mv': sql}

    def test_markers_split_sql_by_marker_name(self, macros):
        views = macros.call('clickhouse__extract_mv_views', MULTIPLE_MV_SQL)

        assert views == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }

    def test_view_order_follows_markers(self, macros):
        sql = """
--second:begin
select 2
--second:end

--first:begin
select 1
--first:end
"""

        assert list(macros.call('clickhouse__extract_mv_views', sql)) == ['second', 'first']

    def test_single_marked_view_not_treated_as_unmarked(self, macros):
        sql = """
--only_mv:begin
select id from people
--only_mv:end
"""

        assert macros.call('clickhouse__extract_mv_views', sql) == {
            'only_mv': '\nselect id from people\n'
        }

    def test_leading_comment_without_colon_does_not_corrupt_first_name(self, macros):
        """Before the fix, the pattern for the name also matched a new line. A `--` comment
        without a colon became a part of the first marker. The name was then
        `publishes\\nengineering\\npeople\\n--mv1`, and the query of mv1 was empty."""
        sql = '-- publishes engineering people\n' + MULTIPLE_MV_SQL

        assert macros.call('clickhouse__extract_mv_views', sql) == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }

    def test_leading_comment_with_colon_does_not_become_view(self, macros):
        sql = '-- note: publishes people\n' + MULTIPLE_MV_SQL

        assert list(macros.call('clickhouse__extract_mv_views', sql)) == ['mv1', 'mv2']

    def test_comments_between_and_after_views_are_ignored(self, macros):
        sql = MULTIPLE_MV_SQL.replace('union all', '-- glue the two together\nunion all')
        sql += '\n-- nothing follows the last view\n'

        assert macros.call('clickhouse__extract_mv_views', sql) == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }

    def test_comments_inside_view_body_are_preserved(self, macros):
        sql = """
--mv1:begin
-- sales people get no alias
select 'N/A' as alias
--mv1:end
"""

        assert macros.call('clickhouse__extract_mv_views', sql) == {
            'mv1': "\n-- sales people get no alias\nselect 'N/A' as alias\n"
        }

    def test_one_space_after_dashes_is_allowed(self, macros):
        sql = """
-- mv1:begin
select 1
-- mv1:end
"""

        assert macros.call('clickhouse__extract_mv_views', sql) == {'mv1': '\nselect 1\n'}

    def test_more_than_one_space_after_dashes_is_not_marker(self, macros):
        """The pattern permits a maximum of one space between `--` and the name. A marker
        with more spaces is not a marker. The macro then gives one view for the full sql."""
        sql = """
--  mv1:begin
select 1
--  mv1:end
"""

        assert macros.call('clickhouse__extract_mv_views', sql) == {'mv': sql}


EPHEMERAL_CTE = " __dbt__cte__staged as (\nselect id, name, department from people\n)"


class TestExtractMvViewsWithEphemeralCtes:
    """dbt injects the CTE of an ephemeral model one time only, before the first marker or
    into the `with` clause of the first view. Each view must have the CTE."""

    def _call(self, macros, sql, ctes=(EPHEMERAL_CTE,)):
        model = {
            'extra_ctes': [{'id': f'model.test.cte_{i}', 'sql': c} for i, c in enumerate(ctes)]
        }
        return macros.call('clickhouse__extract_mv_views', sql, context={'model': model})

    def test_cte_injected_before_first_marker_is_added_to_each_view(self, macros):
        sql = f"""
with{EPHEMERAL_CTE} --mv1:begin
select id from __dbt__cte__staged where department = 'engineering'
--mv1:end
union all
--mv2:begin
select id from __dbt__cte__staged where department = 'sales'
--mv2:end
"""

        assert self._call(macros, sql) == {
            'mv1': f"with{EPHEMERAL_CTE} \nselect id from __dbt__cte__staged where department = 'engineering'\n",
            'mv2': f"with{EPHEMERAL_CTE} \nselect id from __dbt__cte__staged where department = 'sales'\n",
        }

    def test_cte_injected_into_first_view_is_added_to_the_others(self, macros):
        sql = f"""
--mv1:begin
with{EPHEMERAL_CTE}, engineers as (select id from __dbt__cte__staged where department = 'engineering')
select id from engineers
--mv1:end
union all
--mv2:begin
select id from __dbt__cte__staged where department = 'sales'
--mv2:end
"""

        views = self._call(macros, sql)

        assert views['mv1'] == (
            f'\nwith{EPHEMERAL_CTE}, engineers as '
            "(select id from __dbt__cte__staged where department = 'engineering')\n"
            'select id from engineers\n'
        )
        assert views['mv2'] == (
            f"with{EPHEMERAL_CTE} \nselect id from __dbt__cte__staged where department = 'sales'\n"
        )

    def test_cte_is_added_to_with_clause_of_a_later_view(self, macros):
        sql = f"""
with{EPHEMERAL_CTE} --mv1:begin
select id from __dbt__cte__staged
--mv1:end
union all
--mv2:begin
-- sales only
WITH sellers as (select id from __dbt__cte__staged where department = 'sales')
select id from sellers
--mv2:end
"""

        assert self._call(macros, sql)['mv2'] == (
            f'\n-- sales only\nWITH{EPHEMERAL_CTE},'
            " sellers as (select id from __dbt__cte__staged where department = 'sales')\n"
            'select id from sellers\n'
        )

    @pytest.mark.parametrize('comment', ['/* sales only */', '/* sales\n only */', '# sales only'])
    def test_cte_is_added_to_with_clause_after_other_comments(self, macros, comment):
        sql = f"""
with{EPHEMERAL_CTE} --mv1:begin
select id from __dbt__cte__staged
--mv1:end
union all
--mv2:begin
{comment}
WITH sellers as (select id from __dbt__cte__staged where department = 'sales')
select id from sellers
--mv2:end
"""

        assert self._call(macros, sql)['mv2'] == (
            f'\n{comment}\nWITH{EPHEMERAL_CTE},'
            " sellers as (select id from __dbt__cte__staged where department = 'sales')\n"
            'select id from sellers\n'
        )

    def test_several_ctes_are_added_in_dbt_order(self, macros):
        other_cte = ' __dbt__cte__other as (\nselect 1 as id\n)'
        sql = f"""
with{EPHEMERAL_CTE}, {other_cte} --mv1:begin
select id from __dbt__cte__staged
--mv1:end
--mv2:begin
select id from __dbt__cte__other
--mv2:end
"""

        views = self._call(macros, sql, ctes=(EPHEMERAL_CTE, other_cte))

        assert (
            views['mv2'] == f'with{EPHEMERAL_CTE}, {other_cte} \nselect id from __dbt__cte__other\n'
        )

    def test_model_without_markers_is_unchanged(self, macros):
        sql = f'with{EPHEMERAL_CTE} select id from __dbt__cte__staged'

        assert self._call(macros, sql) == {'mv': sql}

    def test_views_are_unchanged_without_ephemeral_models(self, macros):
        assert self._call(macros, MULTIPLE_MV_SQL, ctes=()) == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }
