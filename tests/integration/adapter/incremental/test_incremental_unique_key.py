import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
)

models__expected__one_str__overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    toDate('2022-02-14') as last_visit_date
union all
select 'MA','Suffolk','Boston',toDate('2020-02-12')
union all
select 'NJ','Mercer','Trenton',toDate('2022-01-01')
union all
select 'NY','Kings','Brooklyn',toDate('2021-04-02')
union all
select 'NY','New York','Manhattan',toDate('2021-04-01')
union all
select 'PA','Philadelphia','Philadelphia',toDate('2021-05-21')
union all
select 'CO','Denver','',toDate('2021-06-18')
"""


models__expected__unique_key_list__inplace_overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    toDate('2022-02-14') as last_visit_date
union all
select 'MA','Suffolk','Boston',toDate('2020-02-12')
union all
select 'NJ','Mercer','Trenton',toDate('2022-01-01')
union all
select 'NY','Kings','Brooklyn',toDate('2021-04-02')
union all
select 'NY','New York','Manhattan',toDate('2021-04-01')
union all
select 'PA','Philadelphia','Philadelphia',toDate('2021-05-21')
union all
select 'CO','Denver','',toDate('2021-06-18')
"""


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }
