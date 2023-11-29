import pytest
from dbt.tests.adapter.basic.files import model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.util import run_dbt

uniq_schema = """
version: 2

models:
  - name: "unique_source_one"
    description: "Test table source"
    columns:
      - name: ts
      - name: impid
      - name: value1
"""

uniq_source_model = """
{{config(
        materialized='table',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid']
    )
}}
SELECT now() - toIntervalHour(number) as ts, toInt32(number) as impid, concat('value', toString(number)) as value1
  FROM numbers(100)
"""

uniq_incremental_model = """
{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid'],
        settings={'allow_nullable_key':'1'}
    )
}}
select ts, impid from unique_source_one
{% if is_incremental() %}
where ts >= now() - toIntervalHour(1)
{% endif %}
"""


class TestSimpleIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unique_source_one.sql": uniq_source_model,
            "unique_incremental_one.sql": uniq_incremental_model,
            "schema.yml": uniq_schema,
        }

    def test_simple_incremental(self, project):
        run_dbt(["run", "--select", "unique_source_one"])
        run_dbt(["run", "--select", "unique_incremental_one"])


lw_delete_inc = """
{{ config(
        materialized='incremental',
        order_by=['key1'],
        unique_key='key1',
        incremental_strategy='delete+insert',
        settings={'allow_nullable_key':1}
    )
}}
{% if is_incremental() %}
   select 2 as key1, 500 as key2, 'test' as value UNION ALL
   select 102 as key1, 400 as key2, 'test2' as value
{% else %}
   SELECT toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value FROM numbers(100)
{% endif %}
"""


class TestLWDeleteIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"lw_delete_inc.sql": lw_delete_inc}

    def test_lw_delete(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from lw_delete_inc", fetch="one")
        assert result[0] == 100
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from lw_delete_inc", fetch="one")
        assert result[0] == 101
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from lw_delete_inc", fetch="one")
        assert result[0] == 101


legacy_inc = """
{{ config(
        materialized='incremental',
        order_by=['key1'],
        unique_key='key1',
        incremental_strategy='legacy',
        settings={'allow_nullable_key':1}
    )
}}
{% if is_incremental() %}
   select 2 as key1, 500 as key2, 'test' as value UNION ALL
   select 102 as key1, 400 as key2, 'test2' as value
{% else %}
   SELECT toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value FROM numbers(100)
{% endif %}
"""


class TestLegacyIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"legacy_inc.sql": legacy_inc}

    def test_legacy(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from legacy_inc", fetch="one")
        assert result[0] == 100
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from legacy_inc", fetch="one")
        assert result[0] == 101
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from legacy_inc", fetch="one")
        assert result[0] == 101


compound_key_schema = """
version: 2

models:
  - name: "compound_key_inc"
    description: "Incremental table"
"""

compound_key_inc = """
{{ config(
        materialized='incremental',
        order_by=['key1', 'key2'],
        unique_key='key1, key2',
        incremental_strategy='delete+insert'
    )
}}
{% if is_incremental() %}
   WITH (SELECT max(key1) - 20 FROM compound_key_inc) as old_max
   SELECT assumeNotNull(toUInt64(number + old_max + 1)) as key1, toInt64(-key1) as key2, toString(number + 30) as value FROM numbers(100)
{% else %}
   SELECT toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value FROM numbers(100)
{% endif %}
"""


class TestIncrementalCompoundKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"compound_key_inc.sql": compound_key_inc}

    def test_compound_key(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from compound_key_inc", fetch="one")
        assert result[0] == 100
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from compound_key_inc", fetch="one")
        assert result[0] == 180
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from compound_key_inc", fetch="one")
        assert result[0] == 260


class TestInsertsOnlyIncrementalMaterialization(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_incremental = """
          {{ config(order_by='(some_date, id, name)', inserts_only=True, materialized='incremental', unique_key='id') }}
        """
        incremental_sql = config_materialized_incremental + model_incremental
        return {
            "incremental.sql": incremental_sql,
            "schema.yml": schema_base_yml,
        }
