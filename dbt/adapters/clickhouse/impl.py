import csv
import io
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Union

import agate
from dbt.adapters.base import AdapterConfig, available
from dbt.adapters.base.impl import BaseAdapter, ConstraintSupport, catch_as_completed
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ConstraintType, ModelLevelConstraint
from dbt.contracts.relation import RelationType
from dbt.events.functions import warn_or_error
from dbt.events.types import ConstraintNotSupported
from dbt.exceptions import DbtInternalError, DbtRuntimeError, NotImplementedError
from dbt.utils import executor, filter_null_values

from dbt.adapters.clickhouse.cache import ClickHouseRelationsCache
from dbt.adapters.clickhouse.column import ClickHouseColumn
from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.adapters.clickhouse.relation import ClickHouseRelation

GET_CATALOG_MACRO_NAME = 'get_catalog'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'


@dataclass
class ClickHouseConfig(AdapterConfig):
    engine: str = 'MergeTree()'
    order_by: Optional[Union[List[str], str]] = 'tuple()'
    partition_by: Optional[Union[List[str], str]] = None
    sharding_key: Optional[Union[List[str], str]] = 'rand()'


class ClickHouseAdapter(SQLAdapter):
    Relation = ClickHouseRelation
    Column = ClickHouseColumn
    ConnectionManager = ClickHouseConnectionManager
    AdapterSpecificConfigs = ClickHouseConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    def __init__(self, config):
        BaseAdapter.__init__(self, config)
        self.cache = ClickHouseRelationsCache()

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'String'

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        # We match these type to the Column.TYPE_LABELS for consistency
        return 'Float32' if decimals else 'Int32'

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'Bool'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'Date'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise NotImplementedError('`convert_time_type` is not implemented for this adapter!')

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_cluster_name(self):
        conn = self.connections.get_if_exists()
        if conn.credentials.cluster:
            return f'"{conn.credentials.cluster}"'

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_local_suffix(self):
        conn = self.connections.get_if_exists()
        suffix = conn.credentials.local_suffix
        if suffix:
            if suffix.startswith('_'):
                return f'{suffix}'
            return f'_{suffix}'

    @available
    def clickhouse_db_engine_clause(self):
        conn = self.connections.get_if_exists()
        if conn and conn.credentials.database_engine:
            return f'ENGINE {conn.credentials.database_engine}'
        return ''

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            return compare_versions(version, server_version) > 0
        return False

    @available.parse_none
    def supports_atomic_exchange(self) -> bool:
        conn = self.connections.get_if_exists()
        return conn and conn.handle.atomic_exchange

    @available.parse_none
    def can_exchange(self, schema: str, rel_type: str) -> bool:
        if rel_type != 'table' or not schema or not self.supports_atomic_exchange():
            return False
        ch_db = self.get_ch_database(schema)
        return ch_db and ch_db.engine in ('Atomic', 'Replicated')

    @available.parse_none
    def should_on_cluster(self, materialized: str = '', engine: str = '') -> bool:
        conn = self.connections.get_if_exists()
        if conn and conn.credentials.cluster:
            return ClickHouseRelation.get_on_cluster(conn.credentials.cluster, materialized, engine)
        return ClickHouseRelation.get_on_cluster('', materialized, engine)

    @available.parse_none
    def calculate_incremental_strategy(self, strategy: str) -> str:
        conn = self.connections.get_if_exists()
        if not strategy or strategy == 'default':
            strategy = 'delete_insert' if conn.handle.use_lw_deletes else 'legacy'
        strategy = strategy.replace('+', '_')
        if strategy not in ['legacy', 'append', 'delete_insert']:
            raise DbtRuntimeError(
                f"The incremental strategy '{strategy}' is not valid for ClickHouse"
            )
        if not conn.handle.has_lw_deletes and strategy == 'delete_insert':
            logger.warning(
                'Lightweight deletes are not available, using legacy ClickHouse strategy'
            )
            strategy = 'legacy'
        return strategy

    @available.parse_none
    def s3source_clause(
        self,
        config_name: str,
        s3_model_config: dict,
        bucket: str,
        path: str,
        fmt: str,
        structure: Union[str, list, dict],
        aws_access_key_id: str,
        aws_secret_access_key: str,
        compression: str = '',
    ) -> str:
        s3config = self.config.vars.vars.get(config_name, {})
        s3config.update(s3_model_config)
        structure = structure or s3config.get('structure', '')
        struct = ''
        if structure:
            if isinstance(structure, dict):
                cols = [f'{name} {col_type}' for name, col_type in structure.items()]
                struct = f", '{','.join(cols)}'"
            elif isinstance(structure, list):
                struct = f", '{','.join(structure)}'"
            else:
                struct = f",'{structure}'"
        fmt = fmt or s3config.get('fmt')
        bucket = bucket or s3config.get('bucket', '')
        path = path or s3config.get('path', '')
        url = bucket.replace('https://', '')
        if path:
            if bucket and path and not bucket.endswith('/') and not bucket.startswith('/'):
                path = f'/{path}'
            url = f'{url}{path}'.replace('//', '/')
        url = f'https://{url}'
        access = ''
        if aws_access_key_id and not aws_secret_access_key:
            raise DbtRuntimeError('S3 aws_access_key_id specified without aws_secret_access_key')
        if aws_secret_access_key and not aws_access_key_id:
            raise DbtRuntimeError('S3 aws_secret_access_key specified without aws_access_key_id')
        if aws_access_key_id:
            access = f", '{aws_access_key_id}', '{aws_secret_access_key}'"
        comp = compression or s3config.get('compression', '')
        if comp:
            comp = f"', {comp}'"
        return f"s3('{url}'{access}, '{fmt}'{struct}{comp})"

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={'database': database})
        return schema in (row[0] for row in results)

    def drop_schema(self, relation: BaseRelation) -> None:
        super().drop_schema(relation)
        conn = self.connections.get_if_exists()
        if conn:
            conn.handle.database_dropped(relation.schema)

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
            }
        )

    def list_relations_without_caching(
        self, schema_relation: ClickHouseRelation
    ) -> List[ClickHouseRelation]:
        kwargs = {'schema_relation': schema_relation}
        results = self.execute_macro('list_relations_without_caching', kwargs=kwargs)
        conn_supports_exchange = self.supports_atomic_exchange()

        relations = []
        for row in results:
            name, schema, type_info, db_engine, on_cluster = row
            rel_type = RelationType.View if 'view' in type_info else RelationType.Table
            can_exchange = (
                conn_supports_exchange
                and rel_type == RelationType.Table
                and db_engine in ('Atomic', 'Replicated')
            )

            relation = self.Relation.create(
                database='',
                schema=schema,
                identifier=name,
                type=rel_type,
                can_exchange=can_exchange,
                can_on_cluster=(on_cluster >= 1),
            )
            relations.append(relation)

        return relations

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        return super().get_relation('', schema, identifier)

    @available.parse_none
    def get_ch_database(self, schema: str):
        try:
            results = self.execute_macro('clickhouse__get_database', kwargs={'database': schema})
            if len(results.rows):
                return ClickHouseDatabase(**results.rows[0])
            return None
        except DbtRuntimeError:
            return None

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)

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
            raise DbtRuntimeError(
                f"Expected only one schema in clickhouse _get_one_catalog, found ' f'{schemas}'"
            )
        return super()._get_one_catalog(information_schema, schemas, manifest)

    def get_rows_different_sql(
        self,
        relation_a: ClickHouseRelation,
        relation_b: ClickHouseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: Optional[str] = None,
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
        join_condition = ' AND '.join([f'{alias_a}.{name} = {alias_b}.{name}' for name in names])
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

    @available
    def get_csv_data(self, table):
        csv_funcs = [c.csvify for c in table._column_types]

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        for row in table.rows:
            writer.writerow(tuple(csv_funcs[i](d) for i, d in enumerate(row)))

        return buf.getvalue()

    def run_sql_for_tests(self, sql, fetch, conn):
        client = conn.handle
        try:
            if fetch:
                result = client.query(sql).result_set
            else:
                result = client.command(sql)
            if fetch == "one" and len(result) > 0:
                return result[0]
            if fetch == "all":
                return result
        except BaseException as e:
            logger.error(sql)
            logger.error(e)
            raise
        finally:
            conn.state = 'close'

    @available
    def get_model_settings(self, model):
        settings = model['config'].get('settings', {})
        conn = self.connections.get_if_exists()
        conn.handle.update_model_settings(settings)
        res = []
        for key in settings:
            res.append(f' {key}={settings[key]}')
        return '' if len(res) == 0 else 'SETTINGS ' + ', '.join(res) + '\n'

    @available
    def get_model_query_settings(self, model):
        settings = model['config'].get('query_settings', {})
        res = []
        for key in settings:
            res.append(f' {key}={settings[key]}')
        return '' if len(res) == 0 else 'SETTINGS ' + ', '.join(res) + '\n'

    @available.parse_none
    def get_column_schema_from_query(self, sql: str, *_) -> List[ClickHouseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        conn = self.connections.get_if_exists()
        return conn.handle.columns_in_query(sql)

    @available.parse_none
    def format_columns(self, columns) -> List[Dict]:
        return [{'name': column.name, 'data_type': column.dtype} for column in columns]

    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_columns = []
        for v in raw_columns.values():
            rendered_columns.append(f"{quote_identifier(v['name'])} {v['data_type']}")
            if v.get("constraints"):
                warn_or_error(ConstraintNotSupported(constraint='column', adapter='clickhouse'))
        return rendered_columns

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        if constraint.type == ConstraintType.check and constraint.expression:
            if not constraint.name:
                raise DbtRuntimeError("CHECK Constraint 'name' is required")
            return f"CONSTRAINT {constraint.name} CHECK ({constraint.expression})"
        return None


@dataclass
class ClickHouseDatabase:
    name: str
    engine: str
    comment: str


def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise DbtInternalError(f'Got a row without \'{key}\' column, columns: {row.keys()}')

    return row[key]


def _catalog_filter_schemas(manifest: Manifest) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s) for d, s in manifest.get_used_schemas())

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema) in schemas

    return test


def compare_versions(v1: str, v2: str) -> int:
    v1_parts = v1.split('.')
    v2_parts = v2.split('.')
    for part1, part2 in zip(v1_parts, v2_parts):
        try:
            if int(part1) != int(part2):
                return 1 if int(part1) > int(part2) else -1
        except ValueError:
            raise DbtRuntimeError("Version must consist of only numbers separated by '.'")
    return 0


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
