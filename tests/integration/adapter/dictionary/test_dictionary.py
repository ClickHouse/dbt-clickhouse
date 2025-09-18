"""
test dictionary support in dbt-clickhouse
"""

import json
import os

import pytest
from dbt.tests.util import run_dbt

from tests.integration.adapter.helpers import DEFAULT_RETRY_CONFIG, retry_until_assertion_passes

testing_s3 = os.environ.get('DBT_CH_TEST_INCLUDE_S3', '').lower() in ('1', 'true', 'yes')


PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
""".lstrip()

# This model is parameterized, in a way, by the "run_type" dbt project variable
# This is to be able to switch between different model definitions within
# the same test run and allow us to test the evolution of a materialized view
HACKERS_MODEL = """
{{ config(
       materialized='dictionary',
       fields=[
           ('id', 'Int32'),
           ('name', 'String'),
           ('hacker_alias', 'String')
       ],
       primary_key='id',
       layout='COMPLEX_KEY_HASHED()',
       lifetime='1',
       source_type='clickhouse',
) }}

{% if var('run_type', '') == '' %}
select
    id,
    name,
    case
        when name like 'Dade' then 'crash_override'
        when name like 'Kate' then 'acid burn'
        when name like 'Eugene' then 'the plague'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}

{% else %}

select
    id,
    name,
    case
        -- Dade wasn't always known as 'crash override'!
        when name like 'Dade' and age = 11 then 'zero cool'
        when name like 'Dade' and age != 11 then 'crash override'
        when name like 'Kate' then 'acid burn'
        when name like 'Eugene' then 'the plague'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}
{% endif %}
"""


TAXI_ZONE_DICTIONARY = """
{{ config(
       materialized='dictionary',
       fields=[
           ('LocationID', 'UInt16 DEFAULT 0'),
           ('Borough', 'String'),
           ('Zone', 'String'),
           ('service_zone', 'String'),
       ],
       primary_key='LocationID',
       layout='HASHED()',
       lifetime='MIN 0 MAX 0',
       source_type='http',
       url='https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv',
       format='CSVWithNames'
) }}

select 1
"""


PEOPLE_DICT_MODEL = """
{{ config(
       materialized='dictionary',
       fields=[
           ('id', 'Int32'),
           ('name', 'String'),
       ],
       primary_key='id',
       layout='HASHED()',
       lifetime='1',
       source_type='clickhouse',
       table='people'
) }}

select 1
"""


SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""

RANGE_DICTIONARY = """
{{ config(
       materialized='dictionary',
       fields=[
           ('id', 'UInt8'),
           ('start', 'UInt8'),
           ('stop', 'UInt8'),
           ('value', 'String')
       ],
       primary_key='id',
       layout='RANGE_HASHED()',
       lifetime='MIN 0 MAX 0',
       source_type='clickhouse',
       range='min start max stop'
) }}

select
    c1 as id,
    c2 as start,
    c3 as stop,
    c4 as value
from values(
    (0, 0, 2, 'foo'),
    (0, 3, 5, 'bar')
)
"""


class TestQueryDictionary:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": HACKERS_MODEL,
        }

    def test_create_and_update(self, project):
        run_dbt(["seed"])

        result = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert result[0][1] == "Int32"

        run_dbt()
        result = project.run_sql("select count(distinct id) from hackers", fetch="all")
        assert result[0][0] == 3

        # insert some data and make sure it reaches the target dictionary
        project.run_sql(
            """
        insert into people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'Eugene',40,'malware');
        """
        )
        # force the dictionary to be rebuilt to include the new records in `people`
        project.run_sql("system reload dictionary hackers")

        retry_config = (
            {'max_retries': 30, 'delay': 1}
            if os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes')
            else DEFAULT_RETRY_CONFIG
        )

        def check_count():
            result = project.run_sql("select count(distinct id) from hackers", fetch="all")
            assert result[0][0] == 5

        retry_until_assertion_passes(check_count, **retry_config)

        # re-run dbt but this time with the new MV SQL
        run_vars = {"run_type": "extended_schema"}
        run_dbt(["run", "--vars", json.dumps(run_vars)])
        results = project.run_sql("select distinct hacker_alias from hackers", fetch="all")
        names = set(i[0] for i in results)
        assert names == set(["zero cool", "crash override", "acid burn", "the plague", "N/A"])


class TestTableDictionary:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"people_dict.sql": PEOPLE_DICT_MODEL}

    def test_create(self, project):
        run_dbt(["seed"])
        run_dbt()

        results = project.run_sql("select distinct name from people_dict", fetch="all")
        names = set(i[0] for i in results)
        assert names == set(["Dade", "Kate", "Ksenia"])


class TestHttpDictionary:
    @pytest.fixture(scope="class")
    def models(self):
        return {"taxi_zone_dictionary.sql": TAXI_ZONE_DICTIONARY}

    @pytest.mark.skipif(not testing_s3, reason='Testing S3 disabled')
    def test_create(self, project):
        run_dbt()

        results = project.run_sql(
            "select count(distinct LocationID) from taxi_zone_dictionary", fetch="all"
        )
        assert results[0][0] == 265


class TestRangeDictionary:
    @pytest.fixture(scope="class")
    def models(self):
        return {"range_dictionary.sql": RANGE_DICTIONARY}

    def test_create(self, project):
        run_dbt()

        results = project.run_sql("select dictGet(range_dictionary, 'value', 0, 1)", fetch="all")
        assert results[0][0] == "foo"
        results = project.run_sql("select dictGet(range_dictionary, 'value', 0, 5)", fetch="all")
        assert results[0][0] == "bar"
