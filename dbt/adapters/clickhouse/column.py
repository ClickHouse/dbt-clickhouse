import re
from dataclasses import dataclass, field
from typing import Any, List, Literal, TypeVar

from dbt.adapters.base.column import Column
from dbt_common.exceptions import DbtRuntimeError

Self = TypeVar('Self', bound='ClickHouseColumn')


@dataclass
class ClickHouseColumn(Column):
    TYPE_LABELS = {
        'STRING': 'String',
        'TIMESTAMP': 'DateTime',
        'FLOAT': 'Float32',
        'INTEGER': 'Int32',
    }
    is_nullable: bool = False
    is_low_cardinality: bool = False
    _low_card_regex = re.compile(r'^LowCardinality\((.*)\)$')
    _nullable_regex = re.compile(r'^Nullable\((.*)\)$')
    _fix_size_regex = re.compile(r'FixedString\((.*?)\)')
    _decimal_regex = re.compile(r'Decimal\((\d+), (\d+)\)')

    def __init__(self, column: str, dtype: str) -> None:
        char_size = None
        numeric_precision = None
        numeric_scale = None

        dtype = self._inner_dtype(dtype)

        if dtype.lower().startswith('fixedstring'):
            match_sized = self._fix_size_regex.search(dtype)
            if match_sized:
                char_size = int(match_sized.group(1))

        if dtype.lower().startswith('decimal'):
            match_dec = self._decimal_regex.search(dtype)
            numeric_precision = 0
            numeric_scale = 0
            if match_dec:
                numeric_precision = int(match_dec.group(1))
                numeric_scale = int(match_dec.group(2))

        super().__init__(column, dtype, char_size, numeric_precision, numeric_scale)

    def __repr__(self) -> str:
        return f'<ClickhouseColumn {self.name} ({self.data_type}, is nullable: {self.is_nullable})>'

    @property
    def data_type(self) -> str:
        if self.is_string():
            data_t = self.string_type(self.string_size())
        elif self.is_numeric():
            data_t = self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            data_t = self.dtype

        if self.is_nullable or self.is_low_cardinality:
            data_t = self.nested_type(data_t, self.is_low_cardinality, self.is_nullable)

        return data_t

    def is_string(self) -> bool:
        return self.dtype.lower() in [
            'string',
            'fixedstring',
            'longblob',
            'longtext',
            'tinytext',
            'text',
            'varchar',
            'mediumblob',
            'blob',
            'tinyblob',
            'char',
            'mediumtext',
        ] or self.dtype.lower().startswith('fixedstring')

    def is_integer(self) -> bool:
        return self.dtype.lower().startswith('int') or self.dtype.lower().startswith('uint')

    def is_numeric(self) -> bool:
        return self.dtype.lower().startswith('decimal')

    def is_float(self) -> bool:
        return self.dtype.lower().startswith('float')

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError('Called string_size() on non-string field!')

        if not self.dtype.lower().startswith('fixedstring') or self.char_size is None:
            return 256
        else:
            return int(self.char_size)

    @classmethod
    def string_type(cls, size: int) -> str:
        return 'String'

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        return f'Decimal({precision}, {scale})'

    @classmethod
    def nested_type(cls, dtype: str, is_low_cardinality: bool, is_nullable: bool) -> str:
        template = "{}"
        if is_low_cardinality:
            template = template.format("LowCardinality({})")
        if is_nullable:
            template = template.format("Nullable({})")
        return template.format(dtype)

    def literal(self, value):
        return f'to{self.dtype}({value})'

    def can_expand_to(self, other_column: 'Column') -> bool:
        if not self.is_string() or not other_column.is_string():
            return False

        return other_column.string_size() > self.string_size()

    def _inner_dtype(self, dtype) -> str:
        inner_dtype = dtype.strip()

        if low_card_match := self._low_card_regex.search(inner_dtype):
            self.is_low_cardinality = True
            inner_dtype = low_card_match.group(1)

        if null_match := self._nullable_regex.search(inner_dtype):
            self.is_nullable = True
            inner_dtype = null_match.group(1)

        return inner_dtype


@dataclass(frozen=True)
class ClickHouseColumnChanges:
    on_schema_change: Literal['ignore', 'fail', 'append_new_columns', 'sync_all_columns']
    columns_to_add: List[Column] = field(default_factory=list)
    columns_to_drop: List[Column] = field(default_factory=list)
    columns_to_modify: List[Column] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.columns_to_add or self.columns_to_drop or self.columns_to_modify)

    @property
    def has_schema_changes(self) -> bool:
        return bool(self)

    @property
    def has_sync_changes(self) -> bool:
        return bool(self.columns_to_drop or self.columns_to_modify)

    @property
    def has_conflicting_changes(self) -> bool:
        if self.on_schema_change == 'fail' and self.has_schema_changes:
            return True

        if self.on_schema_change != 'sync_all_columns' and self.has_sync_changes:
            return True

        return False
