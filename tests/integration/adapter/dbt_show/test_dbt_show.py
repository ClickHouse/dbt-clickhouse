import os

import pytest
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowLimit, BaseShowSqlHeader

# `dbt show` works on dbt core v2 (rows are printed), but these upstream tests
# assert on the in-process Python result objects (`results[0].agate_table`,
# `results.args`), which the v2 subprocess shim cannot reconstruct from
# run_results.json — harness incompatibility, not a v2 defect.
pytestmark = pytest.mark.skipif(
    bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY')),
    reason='asserts on in-process agate result objects; N/A for the v2 subprocess shim',
)


class TestShowLimit(BaseShowLimit):
    pass


class TestShowSqlHeader(BaseShowSqlHeader):
    pass
