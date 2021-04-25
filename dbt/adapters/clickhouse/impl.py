from typing import Optional, List, Union, Set, Callable

import agate
import dbt.exceptions
from dataclasses import dataclass
from concurrent.futures import Future

from dbt.contracts.relation import RelationType
from dbt.contracts.graph.manifest import Manifest
from dbt.clients.agate_helper import table_from_rows
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.clickhouse import (
    ClickhouseConnectionManager,
    ClickhouseRelation,
    ClickhouseColumn,
)
from dbt.utils import executor


GET_CATALOG_MACRO_NAME = 'get_catalog'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'


@dataclass
class ClickhouseConfig(AdapterConfig):
    engine: str = 'MergeTree'
    order_by: Optional[Union[List[str], str]] = 'tuple()'
    partition_by: Optional[Union[List[str], str]] = None


class ClickhouseAdapter(SQLAdapter):
    Relation = ClickhouseRelation
    Column = ClickhouseColumn
    ConnectionManager = ClickhouseConnectionManager
    AdapterSpecificConfigs = ClickhouseConfig

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'String'

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return 'Float32' if decimals else 'Int32'

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'UInt8'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'Date'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise dbt.exceptions.NotImplementedException(
            '`convert_time_type` is not implemented for this adapter!'
        )

    def quote(self, identifier):
        return f'{identifier}'

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME, kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def list_relations_without_caching(
        self, schema_relation: ClickhouseRelation
    ) -> List[ClickhouseRelation]:
        kwargs = {'schema_relation': schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from \'show table extended ...\', '
                    f'got {len(row)} values, expected 4'
                )
            _database, name, schema, type_info = row
            rel_type = RelationType.View if 'view' in type_info else RelationType.Table
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=rel_type,
            )
            relations.append(relation)

        return relations

    def get_relation(self, database: str, schema: str, identifier: str):
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def parse_clickhouse_columns(
        self, relation: Relation, raw_rows: List[agate.Row]
    ) -> List[ClickhouseColumn]:
        rows = [dict(zip(row._keys, row._values)) for row in raw_rows]

        return [
            ClickhouseColumn(
                column=column['name'],
                dtype=column['type'],
            )
            for column in rows
        ]

    def get_columns_in_relation(self, relation: Relation) -> List[ClickhouseColumn]:
        rows: List[agate.Row] = super().get_columns_in_relation(relation)

        return self.parse_clickhouse_columns(relation, rows)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in clickhouse _get_one_catalog, found '
                f'{schemas}'
            )

        return super()._get_one_catalog(information_schema, schemas, manifest)

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=['table_schema', 'table_name'],
        )
        return table.where(_catalog_filter_schemas(manifest))

    def get_rows_different_sql(
        self,
        relation_a: ClickhouseRelation,
        relation_b: ClickhouseRelation,
        column_names: Optional[List[str]] = None,
    ) -> str:
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = 'ta'
        alias_b = 'tb'
        columns_csv_a = ', '.join([f'{alias_a}.{name}' for name in names])
        columns_csv_b = ', '.join([f'{alias_b}.{name}' for name in names])
        join_condition = ' AND '.join(
            [f'{alias_a}.{name} = {alias_b}.{name}' for name in names]
        )
        first_column = names[0]

        # Clickhouse doesn't have an EXCEPT operator
        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f'alter table {dst_name} update {dst_column} = {clause}'
        if where_clause is not None:
            clause += f' where {where_clause}'
        return clause


def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise dbt.exceptions.InternalException(
            f'Got a row without \'{key}\' column, columns: {row.keys()}'
        )

    return row[key]


def _catalog_filter_schemas(manifest: Manifest) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in manifest.get_used_schemas())

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test


COLUMNS_EQUAL_SQL = '''
SELECT
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
FROM (
    SELECT
        1 as id,
        (SELECT COUNT(*) as num_rows FROM {relation_a}) -
        (SELECT COUNT(*) as num_rows FROM {relation_b}) as difference
    ) as row_count_diff
INNER JOIN (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} IS NULL
            UNION ALL
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} IS NULL
        ) as missing
    ) as diff_count ON row_count_diff.id = diff_count.id
'''.strip()
