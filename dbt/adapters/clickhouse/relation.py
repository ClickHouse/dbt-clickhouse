from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

from dbt.adapters.base.relation import BaseRelation, Path, Policy, Self
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import deep_merge

from dbt.adapters.clickhouse.query import quote_identifier

NODE_TYPE_SOURCE = 'source'


@dataclass
class ClickHouseQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class ClickHouseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


class ClickHouseRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materialized_view"
    External = "external"
    Ephemeral = "ephemeral"
    Dictionary = "dictionary"


@dataclass(frozen=True, eq=False, repr=False)
class ClickHouseRelation(BaseRelation):
    type: Optional[ClickHouseRelationType] = None
    quote_policy: Policy = field(default_factory=lambda: ClickHouseQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: ClickHouseIncludePolicy())
    quote_character: str = '`'
    can_exchange: bool = False
    can_on_cluster: bool = False

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError(f'Cannot set database {self.database} in clickhouse!')
        self.path.database = ''

    def render(self) -> str:
        return ".".join(quote_identifier(part) for _, part in self._render_iterator() if part)

    def derivative(self, suffix: str, relation_type: Optional[str] = None) -> BaseRelation:
        path = Path(schema=self.path.schema, database='', identifier=self.path.identifier + suffix)
        derivative_type = ClickHouseRelationType(relation_type) if relation_type else self.type
        return ClickHouseRelation(type=derivative_type, path=path)

    def matches(
        self,
        database: Optional[str] = '',
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ):
        if schema:
            raise DbtRuntimeError(f'Passed unexpected schema value {schema} to Relation.matches')
        return self.database == database and self.identifier == identifier

    @property
    def should_on_cluster(self) -> bool:
        if self.include_policy.identifier:
            return self.can_on_cluster
        else:
            # create database/schema on cluster by default
            return True

    @classmethod
    def get_on_cluster(
        cls: Type[Self], cluster: str = '', materialized: str = '', engine: str = ''
    ) -> bool:
        if cluster.strip():
            return (
                materialized in ('view', 'dictionary')
                or 'distributed' in materialized
                or 'Replicated' in engine
            )

        else:
            return False

    @classmethod
    def create_from(
        cls: Type[Self],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> Self:
        quote_policy = kwargs.pop("quote_policy", {})

        config_quoting = relation_config.quoting_dict
        config_quoting.pop("column", None)
        # precedence: kwargs quoting > relation config quoting > base quoting > default quoting
        quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            quoting.quoting,
            config_quoting,
            quote_policy,
        )

        # If the database is set, and the source schema is "defaulted" to the source.name, override the
        # schema with the database instead, since that's presumably what's intended for clickhouse
        schema = relation_config.schema
        can_on_cluster = None
        # We placed a hardcoded const (instead of importing it from dbt-core) in order to decouple the packages
        if relation_config.resource_type == NODE_TYPE_SOURCE:
            if schema == relation_config.source_name and relation_config.database:
                schema = relation_config.database

        else:
            cluster = quoting.credentials.cluster if quoting.credentials.cluster else ''
            materialized = (
                relation_config.config.materialized if relation_config.config.materialized else ''
            )
            engine = (
                relation_config.config.get('engine') if relation_config.config.get('engine') else ''
            )
            can_on_cluster = cls.get_on_cluster(cluster, materialized, engine)

        return cls.create(
            database='',
            schema=schema,
            identifier=relation_config.identifier,
            quote_policy=quote_policy,
            can_on_cluster=can_on_cluster,
            **kwargs,
        )
