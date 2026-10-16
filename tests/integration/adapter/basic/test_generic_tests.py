import pytest
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests

# Upstream's generic_test_seed_yml describes the `base` seed under a `models:`
# key. Python silently tolerates that, but dbt core v2 drops the block
# (dbt1089 NoNodeForYamlKey), so the seed's not_null test never exists and the
# test count assertion fails. `seeds:` is the canonical key and both engines
# accept it.
generic_test_seed_yml = files.generic_test_seed_yml.replace("models:", "seeds:")


class TestGenericTests(BaseGenericTests):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": files.seeds_base_csv,
            "schema.yml": generic_test_seed_yml,
        }
