"""
Common fixtures and constants for materialized view tests.
"""

import json

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
1000,Alfie,10,sales
2000,Bill,20,sales
3000,Charlie,30,sales
""".lstrip()

SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""

VIEW_MODEL_HACKERS = """
{{ config(
       materialized='view'
) }}

select
    id,
    name,
    case
        when name like 'Dade' then 'crash_override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}
where department = 'engineering'
"""

MV_MODEL_HACKERS = """
{{ config(materialized='materialized_view') }}

{%- if not var('catchup', True) %}
{{ config(catchup=False) }}
{%- endif %}

{%- if var('target_table', none) %}
{{ materialization_target_table(ref(var('target_table'))) }}
{%- endif %}

select
    id,
    name,
    case
        when name like 'Dade' then 'crash_override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}
where department = 'engineering'
"""


TARGET_MODEL_HACKERS = """
{{ config(materialized='table') }}

{%- if var('enable_repopulate_from_mvs_on_full_refresh', False) %}
{{ config(repopulate_from_mvs_on_full_refresh=true) }}
{%- endif %}

{% if var('run_type', '') == 'extended_schema' %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias,
    toInt32(0) AS extra_col
WHERE 0
{% else %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias
WHERE 0  -- Creates empty table with correct schema
{% endif %}
"""

MV_MODEL = """
{{ config(materialized='materialized_view') }}

{%- if var('alias', '') %}
{{ config(alias=var('alias')) }}
{%- endif %}

{%- if not var('catchup', True) %}
{{ config(catchup=False) }}
{%- endif %}

{%- if var('target_table', none) %}
{{ materialization_target_table(ref(var('target_table'))) }}
{%- endif %}

select
    p.id,
    p.name,
    'engineering' as department
from {{ source('raw', 'people') }} p
where p.department = 'engineering'
"""

TARGET_MODEL = """
{{ config(materialized='table') }}

{%- if var('enable_repopulate_from_mvs_on_full_refresh', False) %}
{{ config(repopulate_from_mvs_on_full_refresh=true) }}
{%- endif %}

{% if var('run_type', '') == 'extended_schema' %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS department,
    toInt32(0) AS extra_col
WHERE 0
{% else %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS department
WHERE 0  -- Creates empty table with correct schema
{% endif %}
"""


def query_table_type(project, schema, table):
    """Helper to query table engine type from system.tables"""
    table_type = project.run_sql(
        f"""
        select engine from system.tables where database = '{schema}' and name = '{table}'
    """,
        fetch="all",
    )
    return table_type[0][0] if len(table_type) > 0 else None


def refreshable_mv_model(catchup=True, external_target=False, **refreshable):
    """
    Refreshable MV over the people seed. AFTER 1 HOUR counts from creation, so no scheduled
    refresh can fire while a test asserts row counts; only the catchup or the initial refresh
    can have populated the target.
    """
    refreshable = {"interval": "AFTER 1 HOUR", **refreshable}
    if external_target:
        storage = ""
        target = "\n{{ materialization_target_table(ref('hackers_target')) }}\n"
    else:
        storage = "\n       engine='MergeTree()',\n       order_by='(department)',"
        target = ""
    return f"""
{{{{ config(
       materialized='materialized_view',{storage}
       catchup={catchup},
       refreshable={json.dumps(refreshable)}
) }}}}
{target}
select
    department,
    avg(age) as average
from {{{{ source('raw', 'people') }}}}
group by department
"""


def target_row_count(project, table):
    return project.run_sql(f"select count() from {table}", fetch="one")[0]
