"""
Common fixtures and constants for materialized view tests.
"""

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
       materialized='view',
       schema='custom_schema'
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


def query_table_type(project, schema, table):
    """Helper to query table engine type from system.tables"""
    table_type = project.run_sql(
        f"""
        select engine from system.tables where database = '{schema}' and name = '{table}'
    """,
        fetch="all",
    )
    return table_type[0][0] if len(table_type) > 0 else None
