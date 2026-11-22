"""
test UDF creation support for dbt-clickhouse
"""

import os

import dbt.tests.adapter.functions.files as files
import pytest
import yaml
from dbt.adapters.events.types import SQLQuery
from dbt.events.types import JinjaLogWarning
from dbt.tests.adapter.functions.test_udafs import BasicSQLUDAF
from dbt.tests.adapter.functions.test_udfs import (
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    UDFsBasic,
)
from dbt.tests.util import run_dbt
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

MY_UDF_SQL = """
price * 2
"""


def _with_alias(function_yml: str, alias: str) -> str:
    spec = yaml.safe_load(function_yml)
    spec["functions"][0].setdefault("config", {})["alias"] = alias
    return yaml.safe_dump(spec)


class UniqueFunctionName:
    """Helper class for handling function aliasing and lifecycle

    ClickHouse function names are global so we give each test class its own function via `alias`,
    so concurrent workers and CI runs don't overwrite each other's function. also cleanup the
    created function.
    """

    function_name = "price_for_xlarge"

    @pytest.fixture(scope="class")
    def function_alias(self, unique_schema):
        return f"{self.function_name}_{unique_schema}"

    @pytest.fixture(scope="class")
    def functions(self, function_alias):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": _with_alias(files.MY_UDF_YML, function_alias),
        }

    # functions don't belong to the test database, so dropping it doesn't remove them
    @pytest.fixture(scope="class", autouse=True)
    def drop_function(self, project, test_config, function_alias):
        yield
        cluster = test_config["cluster"]
        cluster_clause = f'ON CLUSTER "{cluster}"' if cluster else ""
        project.run_sql(f"DROP FUNCTION IF EXISTS `{function_alias}` {cluster_clause}")


class TestUDFBasics(UniqueFunctionName, UDFsBasic):
    """also runs the full `test_udfs` suite from UDFsBasic"""

    def test_created_under_alias(self, project, function_alias):
        run_dbt(["build"])
        result = project.run_sql(
            f"SELECT count() FROM system.functions WHERE name = '{function_alias}'", fetch="one"
        )
        assert result[0] == 1


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabase(UniqueFunctionName):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }

    # functions aren't part of the Replicated database, so they need ON CLUSTER
    def test_created_on_all_replicas(self, project, test_config, function_alias):
        cluster = test_config["cluster"]
        if not cluster:
            pytest.skip("requires a cluster")

        run_dbt(["build"])

        replicas = project.run_sql(
            f"SELECT count() FROM system.clusters WHERE cluster = '{cluster}'", fetch="one"
        )
        created = project.run_sql(
            f"SELECT count() FROM clusterAllReplicas('{cluster}', system.functions)"
            f" WHERE name = '{function_alias}'",
            fetch="one",
        )
        assert created[0] == replicas[0]


class TestBasicSQLUDAF(UniqueFunctionName, BasicSQLUDAF):
    function_name = "sum_squared"

    @pytest.fixture(scope="class")
    def functions(self, function_alias):
        return {
            "sum_squared.sql": files.SUM_SQUARED_UDAF_SQL,
            "sum_squared.yml": _with_alias(files.SUM_SQUARED_UDAF_SQL_YML, function_alias),
        }

    # ClickHouse has no CREATE AGGREGATE FUNCTION
    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "sum_squared"
            and "CREATE OR REPLACE FUNCTION" in event.data.sql
        )


class TestUnsupportedConfigIgnored(TestUDFBasics):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {
                "+volatility": "deterministic",
                # ClickHouse has no function comments
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    def test_unsupported_config_ignored(self, project):
        warnings = EventCatcher(JinjaLogWarning)
        queries = EventCatcher(
            SQLQuery, predicate=lambda event: event.data.node_info.node_name == "price_for_xlarge"
        )
        run_dbt(["build"], callbacks=[warnings.catch, queries.catch])

        assert len(warnings.caught_events) == 1
        assert "volatility" in warnings.caught_events[0].data.msg

        executed_sql = [event.data.sql.lower() for event in queries.caught_events]
        assert any("create or replace function" in sql for sql in executed_sql)
        assert not any("comment" in sql for sql in executed_sql)


class TestDefaultArgNotSupported(UniqueFunctionName):
    @pytest.fixture(scope="class")
    def functions(self, function_alias):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": _with_alias(files.MY_UDF_WITH_DEFAULT_ARG_YML, function_alias),
        }

    def test_default_arg_error(self, project, function_alias):
        result = run_dbt(["build"], expect_pass=False)
        assert len(result.results) == 1
        assert "Argument `price` of function" in result.results[0].message

        result = project.run_sql(
            f"SELECT count() FROM system.functions WHERE name = '{function_alias}'", fetch="one"
        )
        assert result[0] == 0


class TestGrantsNotSupported(UniqueFunctionName):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"functions": {"+grants": {"select": ["some_user"]}}}

    def test_grants_error(self, project, function_alias):
        result = run_dbt(["build"], expect_pass=False)
        assert len(result.results) == 1
        assert "has grants configured" in result.results[0].message

        result = project.run_sql(
            f"SELECT count() FROM system.functions WHERE name = '{function_alias}'", fetch="one"
        )
        assert result[0] == 0


PRICES_SQL = """
SELECT toFloat64(number + 1) AS price FROM numbers(3)
"""

XLARGE_PRICES_SQL = """
{{ config(materialized='table') }}
SELECT price, {{ function('price_for_xlarge') }}(price) AS xlarge_price
FROM {{ ref('prices') }}
"""


class TestModelUsesFunction(UniqueFunctionName):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "prices.sql": PRICES_SQL,
            "xlarge_prices.sql": XLARGE_PRICES_SQL,
        }

    def test_model_uses_function(self, project):
        # the function has to be built before the model that calls it
        results = run_dbt(["build"])
        order = [r.node.name for r in results]
        assert order.index("price_for_xlarge") < order.index("xlarge_prices")

        rows = project.run_sql(
            f"SELECT xlarge_price FROM {project.test_schema}.xlarge_prices ORDER BY price",
            fetch="all",
        )
        assert [row[0] for row in rows] == [2.0, 4.0, 6.0]


MY_UDF_WITH_SIZE_SQL = """
price * size
"""

MY_UDF_WITH_OVERLOAD_YML = """
functions:
  - name: price_for_xlarge
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
    overloads:
      - defined_in: price_for_xlarge_with_size
        arguments:
          - name: price
            data_type: float
          - name: size
            data_type: int
"""


class TestOverloadsNotSupported(UniqueFunctionName):
    @pytest.fixture(scope="class")
    def functions(self, function_alias):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge_with_size.sql": MY_UDF_WITH_SIZE_SQL,
            "price_for_xlarge.yml": _with_alias(MY_UDF_WITH_OVERLOAD_YML, function_alias),
        }

    def test_overloads_error(self, project, function_alias):
        result = run_dbt(["build"], expect_pass=False)
        assert len(result.results) == 1
        assert "defines overloads, which ClickHouse does not support" in result.results[0].message

        result = project.run_sql(
            f"SELECT count() FROM system.functions WHERE name = '{function_alias}'", fetch="one"
        )
        assert result[0] == 0


class TestPythonUDFNotSupported(PythonUDFNotSupported):
    pass


class TestErrorForUnsupportedType(ErrorForUnsupportedType):
    pass
