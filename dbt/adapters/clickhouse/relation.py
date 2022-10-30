from dataclasses import dataclass
from typing import Optional

import dbt.exceptions
from dbt.adapters.base.relation import BaseRelation, Policy


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
    quote_policy: ClickHouseQuotePolicy = ClickHouseQuotePolicy()
    include_policy: ClickHouseIncludePolicy = ClickHouseIncludePolicy()
    quote_character: str = ''
    can_exchange: bool = False

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise dbt.exceptions.RuntimeException(
                f'Cannot set database {self.database} in clickhouse!'
            )

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise dbt.exceptions.RuntimeException(
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
            raise dbt.exceptions.RuntimeException(
                f'Passed unexpected schema value {schema} to Relation.matches'
            )
        return self.database == database and self.identifier == identifier
