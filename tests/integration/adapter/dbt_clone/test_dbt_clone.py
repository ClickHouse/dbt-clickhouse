import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClonePossible


@pytest.mark.skip("clone not supported")
class TestBaseClonePossible(BaseClonePossible):
    pass
