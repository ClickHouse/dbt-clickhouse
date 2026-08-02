from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from unittest.mock import MagicMock

from dbt.clients.jinja import MacroGenerator
from dbt.context.base import get_context_modules
from dbt.context.exceptions_jinja import wrapped_exports
from dbt_common.clients.jinja import extract_toplevel_blocks
from dbt_common.exceptions.macros import MacroReturn

MACROS_ROOT = Path(__file__).parents[2] / 'dbt' / 'include' / 'clickhouse' / 'macros'


@dataclass
class _MacroSource:
    """The data ``MacroGenerator`` needs from a dbt macro node.

    ``macro_sql`` is the full file. Jinja compiles every macro in it. ``name`` selects
    the macro to call.
    """

    name: str
    macro_sql: str


def _jinja_return(value: Any) -> None:
    """dbt's ``return()``. It stops the macro and supplies a value."""
    raise MacroReturn(value)


class SandboxSafeMock(MagicMock):
    """A mock that dbt's Jinja sandbox can call.

    The sandbox refuses to call an object whose ``unsafe_callable`` or ``alters_data``
    attribute is true. A plain ``MagicMock`` makes a child mock for each of those names,
    and a child mock is always true, so the macro fails with ``SecurityError: ... is not
    safely callable``. This class sets both to ``False``. Child mocks share the type, so
    they are safe to call too.
    """

    unsafe_callable = False
    alters_data = False


class MacroHarness:
    """Calls a macro from ``dbt/include/clickhouse/macros`` by name, and resolves the
    macros it calls in turn.

    The harness runs a macro in dbt's Jinja environment, so ``return()``, ``modules``,
    and calls between macros behave as they do at runtime. It reads every macro file and
    puts each macro into one context, which is what dbt does when it parses a project.
    The harness needs no dbt project and no ClickHouse server.

    Use the ``macros`` fixture (see ``tests/unit/conftest.py``). Call a macro with its
    name. The result is the value that the macro returns, or the sql text that the macro
    writes::

        def test_extract_mv_views(macros):
            assert macros.call('clickhouse__extract_mv_views', 'select 1') == {'mv': 'select 1'}

    Some macros read variables from the dbt context, for example ``adapter``, ``config``,
    ``this``, or ``run_query``. Put these variables in ``context``. Use
    ``SandboxSafeMock`` for each variable::

        def test_on_cluster_clause(macros):
            adapter = SandboxSafeMock()
            adapter.get_clickhouse_cluster_name.return_value = '"test_shard"'
            relation = SandboxSafeMock(should_on_cluster=True)
            rendered = macros.call(
                'on_cluster_clause', relation, False, context={'adapter': adapter}
            )
            assert rendered.strip() == 'ON CLUSTER "test_shard"'
    """

    def __init__(self, macros_root: Path = MACROS_ROOT) -> None:
        self.logs: List[str] = []
        self._context: Dict[str, Any] = {
            'return': _jinja_return,
            'modules': get_context_modules(),
            'exceptions': wrapped_exports(None),
            'log': self._log,
        }
        self._macro_names: Dict[str, Path] = {}
        for path in sorted(macros_root.rglob('*.sql')):
            source = path.read_text()
            for block in extract_toplevel_blocks(
                source, allowed_blocks={'macro'}, collect_raw_data=False
            ):
                name = block.block_name
                if name in self._macro_names:
                    raise ValueError(
                        f'macro {name} is defined twice, in {self._macro_names[name]} and '
                        f'{path}; the harness cannot tell which one a test means'
                    )
                self._macro_names[name] = path
                # all macros share one context dict, so a macro can call any other by name
                self._context[name] = MacroGenerator(_MacroSource(name, source), self._context)

    @property
    def macro_names(self) -> List[str]:
        """The name of every macro that the harness found."""
        return list(self._macro_names)

    def compile(self, macro_name: str) -> None:
        """Compile the file of ``macro_name`` and find the macro, but do not call it."""
        self._context[macro_name].get_macro()

    def call(
        self, macro_name: str, *args: Any, context: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Any:
        """Call ``macro_name``. The result is the return value or the rendered text.

        ``context`` adds dbt context variables, or it replaces them, for this call only.
        The macros that ``macro_name`` calls also read these variables.
        """
        if macro_name not in self._macro_names:
            raise KeyError(f'no macro named {macro_name} under {MACROS_ROOT}')
        with self._patched_context(context or {}):
            return self._context[macro_name](*args, **kwargs)

    @contextmanager
    def _patched_context(self, overrides: Dict[str, Any]) -> Generator[None, None, None]:
        original = {key: self._context[key] for key in overrides if key in self._context}
        self._context.update(overrides)
        try:
            yield
        finally:
            for key in overrides:
                self._context.pop(key, None)
            self._context.update(original)

    def _log(self, msg: Any = '', info: bool = False) -> str:
        self.logs.append(str(msg))
        return ''
