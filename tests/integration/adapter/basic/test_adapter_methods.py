import json

import pytest
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.util import run_dbt


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestBaseCaching(BaseAdapterMethod):
    pass


# check_schema_exists is only reachable from Jinja; both dbt Core 1.x (through
# SQLAdapter) and dbt Core v2 dispatch it to clickhouse__check_schema_exists.
assert_schema_exists_macro = """
{% macro assert_schema_exists(schema, expected) %}
  {% set exists = adapter.check_schema_exists(target.database, schema) %}
  {% if exists != expected %}
    {{ exceptions.raise_compiler_error(
        "check_schema_exists(" ~ schema ~ ") returned " ~ exists ~ ", expected " ~ expected) }}
  {% endif %}
{% endmacro %}
"""


class TestCheckSchemaExists:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"assert_schema_exists.sql": assert_schema_exists_macro}

    def test_check_schema_exists(self, project):
        for schema, expected in (
            (project.test_schema, True),
            (f"{project.test_schema}_missing", False),
        ):
            args = json.dumps({"schema": schema, "expected": expected})
            run_dbt(["run-operation", "assert_schema_exists", "--args", args])
