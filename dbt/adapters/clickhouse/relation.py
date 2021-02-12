import dbt.exceptions

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class ClickhouseQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ClickhouseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ClickhouseRelation(BaseRelation):
    quote_policy: ClickhouseQuotePolicy = ClickhouseQuotePolicy()
    include_policy: ClickhouseIncludePolicy = ClickhouseIncludePolicy()
    quote_character: str = ""

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
