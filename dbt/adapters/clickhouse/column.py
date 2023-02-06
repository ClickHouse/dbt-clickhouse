import re
from dataclasses import dataclass
from typing import Any, TypeVar

from dbt.adapters.base.column import Column
from dbt.exceptions import DbtRuntimeError

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
    _brackets_regex = re.compile(r'^(Nullable|LowCardinality)\((.*)\)$')
    _fix_size_regex = re.compile(r'FixedString\((.*?)\)')
    _decimal_regex = re.compile(r'Decimal\((\d+), (\d+)\)')

    def __init__(self, column: str, dtype: str) -> None:
        char_size = None
        numeric_precision = None
        numeric_scale = None

        inner_dtype = self.match_brackets(dtype)
        if inner_dtype:
            dtype = inner_dtype
            if not self.is_nullable:
                # Support LowCardinality(Nullable(dtype))
                inner_dtype = self.match_brackets(dtype)
                dtype = inner_dtype if inner_dtype else dtype

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
            if self.is_nullable:
                return "Nullable({})".format(data_t)
            return data_t
        elif self.is_numeric():
            data_t = self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
            if self.is_nullable:
                return "Nullable({})".format(data_t)
            return data_t
        else:
            if self.is_nullable:
                return "Nullable({})".format(self.dtype)
            return self.dtype

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

    def literal(self, value):
        return f'to{self.dtype}({value})'

    def can_expand_to(self, other_column: 'Column') -> bool:
        if not self.is_string() or not other_column.is_string():
            return False

        return other_column.string_size() > self.string_size()

    def match_brackets(self, dtype):
        match = self._brackets_regex.search(dtype.strip())
        if match:
            self.is_nullable = match.group(1) == 'Nullable'
            return match.group(2)
