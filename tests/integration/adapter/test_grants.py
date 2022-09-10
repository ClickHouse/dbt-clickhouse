from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestModelGrants(BaseModelGrants):
    pass


class TestIncrementalGrants(BaseIncrementalGrants):
    pass


class TestSeedGrants(BaseSeedGrants):
    pass


class TestInvalidGrants(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "511"

    # ClickHouse doesn't give a very specific error for an invalid privilege
    def privilege_does_not_exist_error(self):
        return "Syntax error"


class TestSnapshotGrants(BaseSnapshotGrants):
    pass
