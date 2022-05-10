from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols


class TestBaseSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestEmptyClickHouseAdapter(BaseEmpty):
    pass


class TestIncrementalClickHouseAdapter(BaseIncremental):
    pass


class TestSnapshotTimestampMyAdapter(BaseSnapshotTimestamp):
    pass


class TestSnapshotCheckColsMyAdapter(BaseSnapshotCheckCols):
    pass
