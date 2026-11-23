import pytest


# The upstream dbt_clone fixtures grant to "{{ env_var('DBT_TEST_USER_1') }}" at
# parse time; requesting ch_test_users here creates per-class users and exports
# the DBT_TEST_USER_1..3 variables before any test in this package runs dbt.
@pytest.fixture(scope="class", autouse=True)
def ensure_ch_test_users(ch_test_users):
    pass
