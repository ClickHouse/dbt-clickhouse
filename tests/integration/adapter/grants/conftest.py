import pytest


# The upstream grants base classes read DBT_TEST_USER_1..3 from the environment:
# BaseGrants.get_test_users at class setup, and the model/schema fixtures via
# env_var() at parse time. Requesting ch_test_users here creates per-class users
# and exports those variables first — conftest-level autouse fixtures are ordered
# before same-scope autouse fixtures defined on the test classes, so this resolves
# ahead of BaseGrants.get_test_users.
@pytest.fixture(scope="class", autouse=True)
def ensure_ch_test_users(ch_test_users):
    pass
