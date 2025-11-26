import os
from unittest import mock

import pytest
from dbt.tests.adapter.sample_mode.test_sample_mode import (
    BaseSampleModeTest,
)
from dbt.tests.util import run_dbt

# input_model_sql overrided as the `-0` part at the end of `TIMESTAMP '2025-01-01 01:25:00-0'` is not compatible with CH
input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2025-01-01 01:25:00' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00' as event_time
"""


class TestSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to be sampled, including any {{ config(..) }}.
        event_time is a required configuration of this input
        """
        return input_model_sql

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_SAMPLE_MODE": "True"})
    def test_sample_mode_with_range(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample", "{'start': '2025-01-02', 'end': '2025-01-03'}"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=1,
        )
