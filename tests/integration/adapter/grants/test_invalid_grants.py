from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants


class TestInvalidGrants(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "511"

    # ClickHouse doesn't give a very specific error for an invalid privilege
    def privilege_does_not_exist_error(self):
        return "Syntax error"
