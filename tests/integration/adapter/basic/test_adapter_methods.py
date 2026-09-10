import os

import pytest
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class SerializedAdapterMethod(BaseAdapterMethod):
    # BaseAdapterMethod's model drops and recreates its own schema (a ClickHouse
    # database) mid-run. dbt core v2 renders nodes in parallel without waiting
    # for ref-dependencies, so the test node's get_columns_in_relation(ref('model'))
    # can execute inside the drop window — and with the connection default
    # database set (new ADBC driver), every request in that window fails with
    # UNKNOWN_DATABASE. Profile threads=1 does NOT bound v2's render parallelism
    # (verified: race still fires); DBT_NO_PARALLEL pins the v2 runtime to a
    # single worker, which does. Python dbt ignores the variable. Remove when
    # v2 renders in DAG order; details in fusion-work planning note
    # "Flaky: TestBaseAdapterMethod, TestBaseCaching".
    @pytest.fixture(scope="class", autouse=True)
    def serialize_dbt_core_v2(self):
        old = os.environ.get("DBT_NO_PARALLEL")
        os.environ["DBT_NO_PARALLEL"] = "true"
        yield
        if old is None:
            os.environ.pop("DBT_NO_PARALLEL", None)
        else:
            os.environ["DBT_NO_PARALLEL"] = old


class TestBaseAdapterMethod(SerializedAdapterMethod):
    pass


class TestBaseCaching(SerializedAdapterMethod):
    pass
