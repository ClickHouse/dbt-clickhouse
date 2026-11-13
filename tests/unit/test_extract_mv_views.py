import pytest
from dbt.compilation import inject_ctes_into_sql
from dbt.contracts.graph.nodes import InjectedCTE
from dbt_common.exceptions import CompilationError

MULTIPLE_MV_SQL = """
--mv1:begin
select id, name from people where department = 'engineering'
--mv1:end

union all

--mv2:begin
select id, name from people where department = 'sales'
--mv2:end
"""


def extract_mv_views(macros, sql):
    """dbt always gives the macro the model, here one without ephemeral models."""
    return macros.call(
        'clickhouse__extract_mv_views', sql, context={'model': {'name': 'roles', 'extra_ctes': []}}
    )


class TestExtractMvViews:
    def test_no_markers_yields_single_mv_suffix(self, macros):
        """The key `mv` is a suffix on the relation of the model. The view is `<model>_mv`."""
        sql = 'select id, name from people'
        assert extract_mv_views(macros, sql) == {'mv': sql}

    def test_markers_split_sql_by_marker_name(self, macros):
        views = extract_mv_views(macros, MULTIPLE_MV_SQL)

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

        assert list(extract_mv_views(macros, sql)) == ['second', 'first']

    def test_single_marked_view_not_treated_as_unmarked(self, macros):
        sql = """
--only_mv:begin
select id from people
--only_mv:end
"""

        assert extract_mv_views(macros, sql) == {'only_mv': '\nselect id from people\n'}

    def test_leading_comment_without_colon_does_not_corrupt_first_name(self, macros):
        """Before the fix, the pattern for the name also matched a new line. A `--` comment
        without a colon became a part of the first marker. The name was then
        `publishes\\nengineering\\npeople\\n--mv1`, and the query of mv1 was empty."""
        sql = '-- publishes engineering people\n' + MULTIPLE_MV_SQL

        assert extract_mv_views(macros, sql) == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }

    def test_leading_comment_with_colon_does_not_become_view(self, macros):
        sql = '-- note: publishes people\n' + MULTIPLE_MV_SQL

        assert list(extract_mv_views(macros, sql)) == ['mv1', 'mv2']

    def test_comments_between_and_after_views_are_ignored(self, macros):
        sql = MULTIPLE_MV_SQL.replace('union all', '-- glue the two together\nunion all')
        sql += '\n-- nothing follows the last view\n'

        assert extract_mv_views(macros, sql) == {
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

        assert extract_mv_views(macros, sql) == {
            'mv1': "\n-- sales people get no alias\nselect 'N/A' as alias\n"
        }

    def test_one_space_after_dashes_is_allowed(self, macros):
        sql = """
-- mv1:begin
select 1
-- mv1:end
"""

        assert extract_mv_views(macros, sql) == {'mv1': '\nselect 1\n'}

    def test_more_than_one_space_after_dashes_is_not_marker(self, macros):
        """The pattern permits a maximum of one space between `--` and the name. A marker
        with more spaces is not a marker. The macro then gives one view for the full sql."""
        sql = """
--  mv1:begin
select 1
--  mv1:end
"""

        assert extract_mv_views(macros, sql) == {'mv': sql}

    @pytest.mark.parametrize('name', ['mv(1)', 'mv+1', 'mv.1'])
    def test_name_with_regex_characters_is_matched_literally(self, macros, name):
        """The marker pattern accepts any name without a colon or white space, so the name
        is escaped before it is used to find the end marker."""
        sql = f"""
--{name}:begin
select 1
--{name}:end
"""

        assert extract_mv_views(macros, sql) == {name: '\nselect 1\n'}


EPHEMERAL_CTE = InjectedCTE(
    id='model.test.staged',
    sql=' __dbt__cte__staged as (\nselect id, name, department from people\n)',
)
OTHER_CTE = InjectedCTE(id='model.test.other', sql=' __dbt__cte__other as (\nselect 1 as id\n)')


class TestExtractMvViewsWithEphemeralCtes:
    """dbt injects the CTEs of ephemeral models one time only. The tests make the sql of the
    model with `inject_ctes_into_sql` from dbt-core, so the CTEs are where dbt puts them."""

    def _call(self, macros, sql, ctes=(EPHEMERAL_CTE,)):
        model = {'name': 'roles', 'extra_ctes': [cte.to_dict() for cte in ctes]}
        return macros.call(
            'clickhouse__extract_mv_views',
            inject_ctes_into_sql(sql, list(ctes)),
            context={'model': model},
        )

    def test_ctes_before_first_marker_are_added_to_each_view(self, macros):
        """No view has a `with` clause, thus dbt adds `with <ctes> ` before the first marker."""
        sql = """
--mv1:begin
select id from __dbt__cte__staged where department = 'engineering'
--mv1:end
union all
--mv2:begin
select id from __dbt__cte__staged where department = 'sales'
--mv2:end
"""

        assert self._call(macros, sql) == {
            'mv1': f"with{EPHEMERAL_CTE.sql} \nselect id from __dbt__cte__staged where department = 'engineering'\n",
            'mv2': f"with{EPHEMERAL_CTE.sql} \nselect id from __dbt__cte__staged where department = 'sales'\n",
        }

    def test_ctes_before_leading_comment_are_added_to_each_view(self, macros):
        """dbt adds the CTEs before the first token, which can be a comment before the first
        marker."""
        sql = (
            '-- publishes engineering people\n'
            + """
--mv1:begin
select id from __dbt__cte__staged
--mv1:end
--mv2:begin
select 2 as id
--mv2:end
"""
        )

        assert self._call(macros, sql) == {
            'mv1': f'with{EPHEMERAL_CTE.sql} \nselect id from __dbt__cte__staged\n',
            'mv2': f'with{EPHEMERAL_CTE.sql} \nselect 2 as id\n',
        }

    def test_several_ctes_are_added_in_dbt_order(self, macros):
        sql = """
--mv1:begin
select id from __dbt__cte__staged
--mv1:end
--mv2:begin
select id from __dbt__cte__other
--mv2:end
"""

        views = self._call(macros, sql, ctes=(EPHEMERAL_CTE, OTHER_CTE))

        assert views['mv2'] == (
            f'with{EPHEMERAL_CTE.sql}, {OTHER_CTE.sql} \nselect id from __dbt__cte__other\n'
        )

    def test_first_view_with_own_with_clause_raises(self, macros):
        """dbt adds the CTEs to the `with` clause of mv1, thus mv2 does not have them."""
        sql = """
--mv1:begin
with engineers as (select id from __dbt__cte__staged where department = 'engineering')
select id from engineers
--mv1:end
union all
--mv2:begin
select id from __dbt__cte__staged where department = 'sales'
--mv2:end
"""

        with pytest.raises(CompilationError) as excinfo:
            self._call(macros, sql)

        message = str(excinfo.value)
        assert (
            'The materialized view `mv2` of model `roles` selects from an ephemeral model'
            in message
        )
        assert 'when a view has its own WITH clause' in message
        assert 'explicit target table' in message

    def test_only_later_view_with_own_with_clause_raises(self, macros):
        """dbt adds the CTEs to the `with` clause of mv2, not before the first marker, thus mv1
        and the query of the target table do not have them."""
        sql = """
--mv1:begin
select id from __dbt__cte__staged where department = 'engineering'
--mv1:end
union all
--mv2:begin
WITH sellers as (select id from __dbt__cte__staged where department = 'sales')
select id from sellers
--mv2:end
"""

        with pytest.raises(CompilationError, match='The materialized view `mv1` of model `roles`'):
            self._call(macros, sql)

    def test_view_with_own_with_clause_is_unchanged_if_other_views_need_no_ctes(self, macros):
        """Only mv1 selects from the ephemeral model, and dbt added the CTEs to its `with`
        clause. This works without the adapter's help, so the views are the same as before."""
        sql = """
--mv1:begin
with engineers as (select id from __dbt__cte__staged where department = 'engineering')
select id from engineers
--mv1:end
union all
--mv2:begin
select id from people where department = 'sales'
--mv2:end
"""

        assert self._call(macros, sql) == {
            'mv1': (
                f'\nwith {EPHEMERAL_CTE.sql}, engineers as '
                "(select id from __dbt__cte__staged where department = 'engineering')\n"
                'select id from engineers\n'
            ),
            'mv2': "\nselect id from people where department = 'sales'\n",
        }

    def test_mention_in_comment_of_other_view_raises(self, macros):
        """The check is a plain search for the CTE prefix, so a view that only names the
        ephemeral model in a comment also raises. The user removes the mention."""
        sql = """
--mv1:begin
with engineers as (select id from __dbt__cte__staged where department = 'engineering')
select id from engineers
--mv1:end
union all
--mv2:begin
-- not read from __dbt__cte__staged
select id from people where department = 'sales'
--mv2:end
"""

        with pytest.raises(CompilationError, match='The materialized view `mv2` of model `roles`'):
            self._call(macros, sql)

    def test_model_without_markers_is_unchanged(self, macros):
        sql = 'select id from __dbt__cte__staged'

        assert self._call(macros, sql) == {'mv': inject_ctes_into_sql(sql, [EPHEMERAL_CTE])}

    def test_views_are_unchanged_without_ephemeral_models(self, macros):
        assert self._call(macros, MULTIPLE_MV_SQL, ctes=()) == {
            'mv1': "\nselect id, name from people where department = 'engineering'\n",
            'mv2': "\nselect id, name from people where department = 'sales'\n",
        }
