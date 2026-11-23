from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter, Path, Policy, Self
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import deep_merge

NODE_TYPE_SOURCE = 'source'
NODE_TYPE_FUNCTION = 'function'


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
    Function = "function"


@dataclass(frozen=True, eq=False, repr=False)
class ClickHouseRelation(BaseRelation):
    type: Optional[ClickHouseRelationType] = None
    quote_policy: Policy = field(default_factory=lambda: ClickHouseQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: ClickHouseIncludePolicy())
    quote_character: str = '`'
    can_exchange: bool = False
    can_on_cluster: bool = False
    require_alias: bool = False
    is_temporary: bool = False
    mvs_pointing_to_it: List[Dict[str, str]] = field(
        default_factory=list
    )  # List of {'schema', 'name', 'sql'}
    # Populated from the deployed DDL when the relation is loaded from the database cache;
    # relations built any other way default to False.
    is_refreshable: bool = False
    refreshable_append: bool = False

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError(f'Cannot set database {self.database} in clickhouse!')
        self.path.database = ''

    def render(self) -> str:
        return ".".join(quote_identifier(part) for _, part in self._render_iterator() if part)

    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        return self.render_event_time_filter(
            event_time_filter.field_name, event_time_filter.start, event_time_filter.end
        )

    @staticmethod
    def render_event_time_filter(
        field_name: str, start: Optional[datetime], end: Optional[datetime]
    ) -> str:
        """
        Renders `start <= field_name < end`, used for both upstream refs and microbatch deletes.
        Either bound may be None; returns "" if both are.
        """
        # dbt batch boundaries are UTC; a typed literal compares with every date and time column type
        bounds = []
        if start:
            bounds.append(
                f"{field_name} >= toDateTime64('{start.strftime('%Y-%m-%d %H:%M:%S')}', 0, 'UTC')"
            )
        if end:
            bounds.append(
                f"{field_name} < toDateTime64('{end.strftime('%Y-%m-%d %H:%M:%S')}', 0, 'UTC')"
            )
        return ' and '.join(bounds)

    def derivative(
        self,
        suffix: str,
        relation_type: Optional[str] = None,
        interpret_suffix_as_full_identifier: bool = False,
    ) -> BaseRelation:
        new_identifier = (
            suffix if interpret_suffix_as_full_identifier else self.path.identifier + suffix
        )
        path = Path(schema=self.path.schema, database='', identifier=new_identifier)
        derivative_type = ClickHouseRelationType(relation_type) if relation_type else self.type
        return ClickHouseRelation(
            type=derivative_type, path=path, can_on_cluster=self.can_on_cluster
        )

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
        cls: Type[Self],
        cluster: str = '',
        database_engine: Optional[str] = None,
    ) -> bool:
        # not using ternary expression for simplicity
        database_engine = database_engine or ''
        if not cluster.strip() or 'replicated' in database_engine.lower():
            return False
        else:
            return True

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

        # We placed a hardcoded const (instead of importing it from dbt-core) in order to decouple the packages
        if relation_config.resource_type == NODE_TYPE_SOURCE:
            if schema == relation_config.source_name and relation_config.database:
                schema = relation_config.database
        elif relation_config.resource_type == NODE_TYPE_FUNCTION:
            # ClickHouse functions are global, not part of a database
            schema = None

        return cls.create(
            database='',
            schema=schema,
            identifier=relation_config.identifier,
            quote_policy=quote_policy,
            can_on_cluster=cls._can_on_cluster(quoting, relation_config),
            **kwargs,
        )

    @classmethod
    def _can_on_cluster(cls, quoting: HasQuoting, relation_config: RelationConfig) -> bool:
        # source() and function() calls pass dbt's quoting-only config, which has no credentials
        credentials = getattr(quoting, 'credentials', None)
        if not credentials or not credentials.cluster:
            return False
        if relation_config.resource_type == NODE_TYPE_SOURCE:
            return False
        if str(relation_config.config.get("disable_on_cluster")).lower() == "true":
            return False
        # a replicated database replicates ddl itself, but functions aren't part of a database
        if relation_config.resource_type == NODE_TYPE_FUNCTION:
            return True
        return cls.get_on_cluster(credentials.cluster, credentials.database_engine)
