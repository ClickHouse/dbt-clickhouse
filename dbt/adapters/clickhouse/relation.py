from dataclasses import dataclass, field
from typing import Optional

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import DbtRuntimeError


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

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError(f'Cannot set database {self.database} in clickhouse!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                'Got a clickhouse relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ):
        if schema:
            raise DbtRuntimeError(f'Passed unexpected schema value {schema} to Relation.matches')
        return self.database == database and self.identifier == identifier
