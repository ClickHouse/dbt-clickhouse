from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

from dbt.adapters.base.relation import BaseRelation, Policy, Self
from dbt.contracts.graph.nodes import ManifestNode, SourceDefinition
from dbt.contracts.relation import HasQuoting
from dbt.exceptions import DbtRuntimeError
from dbt.utils import deep_merge, merge


@dataclass
class ClickHouseQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ClickHouseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ClickHouseRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: ClickHouseQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: ClickHouseIncludePolicy())
    quote_character: str = ''
    can_exchange: bool = False
    can_on_cluster: bool = False

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError(f'Cannot set database {self.database} in clickhouse!')
        self.path.database = ''

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                'Got a clickhouse relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()

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
            return 'view' == materialized or 'distributed' in materialized or 'Replicated' in engine
        else:
            return False

    @classmethod
    def create_from_source(cls: Type[Self], source: SourceDefinition, **kwargs: Any) -> Self:
        source_quoting = source.quoting.to_dict(omit_none=True)
        source_quoting.pop("column", None)
        quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            source_quoting,
            kwargs.get("quote_policy", {}),
        )

        # If the database is set, and the source schema is "defaulted" to the source.name, override the
        # schema with the database instead, since that's presumably what's intended for clickhouse
        schema = source.schema
        if schema == source.source_name and source.database:
            schema = source.database

        return cls.create(
            database='',
            schema=schema,
            identifier=source.identifier,
            quote_policy=quote_policy,
            **kwargs,
        )

    @classmethod
    def create_from_node(
        cls: Type[Self],
        config: HasQuoting,
        node: ManifestNode,
        quote_policy: Optional[Dict[str, bool]] = None,
        **kwargs: Any,
    ) -> Self:
        if quote_policy is None:
            quote_policy = {}

        quote_policy = merge(config.quoting, quote_policy)

        cluster = config.credentials.cluster if config.credentials.cluster else ''
        materialized = node.get_materialization() if node.get_materialization() else ''
        engine = node.config.get('engine') if node.config.get('engine') else ''
        can_on_cluster = cls.get_on_cluster(cluster, materialized, engine)

        return cls.create(
            database='',
            schema=node.schema,
            identifier=node.alias,
            quote_policy=quote_policy,
            can_on_cluster=can_on_cluster,
            **kwargs,
        )
