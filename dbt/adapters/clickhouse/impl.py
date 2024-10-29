import csv
import io
from dataclasses import dataclass
from multiprocessing.context import SpawnContext
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from dbt.adapters.base import AdapterConfig, available
from dbt.adapters.base.impl import BaseAdapter, ConstraintSupport
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.contracts.relation import Path, RelationConfig
from dbt.adapters.events.types import ConstraintNotSupported
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType, ModelLevelConstraint
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError, NotImplementedError
from dbt_common.utils import filter_null_values

from dbt.adapters.clickhouse.cache import ClickHouseRelationsCache
from dbt.adapters.clickhouse.column import ClickHouseColumn, ClickHouseColumnChanges
from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager
from dbt.adapters.clickhouse.errors import (
    schema_change_datatype_error,
    schema_change_fail_error,
    schema_change_missing_source_error,
)
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.adapters.clickhouse.relation import ClickHouseRelation, ClickHouseRelationType
from dbt.adapters.clickhouse.util import compare_versions

if TYPE_CHECKING:
    import agate

GET_CATALOG_MACRO_NAME = 'get_catalog'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'


@dataclass
class ClickHouseConfig(AdapterConfig):
    engine: str = 'MergeTree()'
    order_by: Optional[Union[List[str], str]] = 'tuple()'
    partition_by: Optional[Union[List[str], str]] = None
    sharding_key: Optional[Union[List[str], str]] = 'rand()'
    ttl: Optional[Union[List[str], str]] = None


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

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Unsupported),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Unsupported),
        }
    )

    def __init__(self, config, mp_context: SpawnContext):
        BaseAdapter.__init__(self, config, mp_context)
        self.cache = ClickHouseRelationsCache()

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'String'

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        # We match these type to the Column.TYPE_LABELS for consistency
        return 'Float32' if decimals else 'Int32'

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Bool'

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Date'

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
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

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_local_db_prefix(self):
        conn = self.connections.get_if_exists()
        prefix = conn.credentials.local_db_prefix
        if prefix:
            if prefix.endswith('_'):
                return f'{prefix}'
            return f'{prefix}_'
        return ''

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
        if strategy not in ['legacy', 'append', 'delete_insert', 'insert_overwrite']:
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
    def check_incremental_schema_changes(
        self, on_schema_change, existing, target_sql
    ) -> ClickHouseColumnChanges:
        if on_schema_change not in ('fail', 'ignore', 'append_new_columns', 'sync_all_columns'):
            raise DbtRuntimeError(
                "Only `fail`, `ignore`, `append_new_columns`, and `sync_all_columns` supported for `on_schema_change`."
            )

        source = self.get_columns_in_relation(existing)
        source_map = {column.name: column for column in source}
        target = self.get_column_schema_from_query(target_sql)
        target_map = {column.name: column for column in target}

        source_not_in_target = [column for column in source if column.name not in target_map.keys()]
        target_not_in_source = [column for column in target if column.name not in source_map.keys()]
        target_in_source = [column for column in target if column.name in source_map.keys()]
        changed_data_types = []
        for column in target_in_source:
            source_column = source_map.get(column.name)
            if source_column is not None and column.dtype != source_column.dtype:
                changed_data_types.append(column)

        clickhouse_column_changes = ClickHouseColumnChanges(
            columns_to_add=target_not_in_source,
            columns_to_drop=source_not_in_target,
            columns_to_modify=changed_data_types,
            on_schema_change=on_schema_change,
        )

        if clickhouse_column_changes.has_conflicting_changes:
            raise DbtRuntimeError(
                schema_change_fail_error.format(
                    source_not_in_target, target_not_in_source, changed_data_types
                )
            )

        return clickhouse_column_changes

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
        role_arn: str,
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
        extra_credentials = ''
        if role_arn:
            extra_credentials = f", extra_credentials(role_arn='{role_arn}')"
        return f"s3('{url}'{access}, '{fmt}'{struct}{comp}{extra_credentials})"

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
            if 'view' in type_info:
                rel_type = ClickHouseRelationType.View
            elif type_info == 'dictionary':
                rel_type = ClickHouseRelationType.Dictionary
            else:
                rel_type = ClickHouseRelationType.Table
            can_exchange = (
                conn_supports_exchange
                and rel_type == ClickHouseRelationType.Table
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

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple["agate.Table", List[Exception]]:
        from dbt_common.clients.agate_helper import empty_table

        relations = self._get_catalog_relations(relation_configs)
        schemas = set(relation.schema for relation in relations)
        if schemas:
            catalog = self._get_one_catalog(InformationSchema(Path()), schemas, used_schemas)
        else:
            catalog = empty_table()
        return catalog, []

    def get_filtered_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
        relations: Optional[Set[BaseRelation]] = None,
    ):
        catalog, exceptions = self.get_catalog(relation_configs, used_schemas)
        if relations and catalog:
            relation_map = {(r.schema, r.identifier) for r in relations}

            def in_map(row: "agate.Row"):
                s = _expect_row_value("table_schema", row)
                i = _expect_row_value("table_name", row)
                return (s, i) in relation_map

            catalog = catalog.where(in_map)
        return catalog, exceptions

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
        materialization_type = model['config'].get('materialized')
        conn = self.connections.get_if_exists()
        conn.handle.update_model_settings(settings, materialization_type)
        res = []
        for key in settings:
            res.append(f' {key}={settings[key]}')
        settings_str = '' if len(res) == 0 else 'SETTINGS ' + ', '.join(res) + '\n'
        return f"""
                    -- end_of_sql
                    {settings_str}
                    """

    @available
    def get_model_query_settings(self, model):
        settings = model['config'].get('query_settings', {})
        res = []
        for key in settings:
            res.append(f' {key}={settings[key]}')

        if len(res) == 0:
            return ''
        else:
            settings_str = 'SETTINGS ' + ', '.join(res) + '\n'
            return f"""
            -- settings_section
            {settings_str}
            """

    @available.parse_none
    def get_column_schema_from_query(self, sql: str, *_) -> List[ClickHouseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        conn = self.connections.get_if_exists()
        return conn.handle.columns_in_query(sql)

    @available.parse_none
    def format_columns(self, columns) -> List[Dict]:
        return [{'name': column.name, 'data_type': column.data_type} for column in columns]

    @available
    def get_credentials(self, connection_overrides) -> Dict:
        conn = self.connections.get_if_exists()
        if conn is None or conn.credentials is None:
            return dict()
        credentials = {
            'user': conn.credentials.user,
            'password': conn.credentials.password,
            'database': conn.credentials.database,
            'host': conn.credentials.host,
            'port': conn.credentials.port,
        }
        credentials.update(connection_overrides)

        for key in connection_overrides.keys():
            if not credentials[key]:
                credentials.pop(key)

        return credentials

    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_columns = []
        for v in raw_columns.values():
            codec = f"CODEC({_codec})" if (_codec := v.get('codec')) else ""
            rendered_columns.append(
                f"{quote_identifier(v['name'])} {v['data_type']} {codec}".rstrip()
            )
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


def _expect_row_value(key: str, row: "agate.Row"):
    if key not in row.keys():
        raise DbtInternalError(f'Got a row without \'{key}\' column, columns: {row.keys()}')

    return row[key]


def _catalog_filter_schemas(
    used_schemas: FrozenSet[Tuple[str, str]]
) -> Callable[["agate.Row"], bool]:
    """Return a function that takes a row and decides if the row should be
    included in the catalog output.
    """
    schemas = frozenset((d.lower(), s.lower()) for d, s in used_schemas)

    def test(row: "agate.Row") -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema) in schemas

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
