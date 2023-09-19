import os

import pytest
from dbt.tests.adapter.aliases.fixtures import (
    MODELS__ALIAS_IN_PROJECT_SQL,
    MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
    MODELS__SCHEMA_YML,
)
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentDatabases,
    BaseSameAliasDifferentSchemas,
)
from dbt.tests.util import relation_from_name, run_dbt

MODELS__DISTRIBUTED_FOO_ALIAS_SQL = """

{{
    config(
        alias='foo',
        materialized='distributed_table'
    )
}}

select {{ string_literal(this.name) }} as tablename

"""

MODELS__DISTRIBUTED_REF_FOO_ALIAS_SQL = """

{{
    config(
        materialized='distributed_table'
    )
}}

with trigger_ref as (

  -- we should still be able to ref a model by its filepath
  select * from {{ ref('foo_alias') }}

)

-- this name should still be the filename
select {{ string_literal(this.name) }} as tablename

"""


class TestAliases(BaseAliases):
    pass


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    pass


class TestDistributedAliases(BaseAliases):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": MODELS__SCHEMA_YML,
            "foo_alias.sql": MODELS__DISTRIBUTED_FOO_ALIAS_SQL,
            "alias_in_project.sql": MODELS__ALIAS_IN_PROJECT_SQL,
            "alias_in_project_with_override.sql": MODELS__ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL,
            "ref_foo_alias.sql": MODELS__DISTRIBUTED_REF_FOO_ALIAS_SQL,
        }

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_alias_model_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4

        cluster = project.test_config['cluster']
        relation = relation_from_name(project.adapter, "foo")

        result = project.run_sql(
            f"select max(tablename) AS tablename From clusterAllReplicas('{cluster}', {relation}_local) ",
            fetch="one",
        )
        assert result[0] == "foo"

        relation = relation_from_name(project.adapter, "ref_foo_alias")
        result = project.run_sql(
            f"select max(tablename) AS tablename From clusterAllReplicas('{cluster}', {relation}_local) ",
            fetch="one",
        )
        assert result[0] == "ref_foo_alias"

        run_dbt(["test"])
