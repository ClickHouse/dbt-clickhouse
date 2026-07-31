import pytest

from tests.unit.macro_harness import MacroHarness


@pytest.fixture
def macros() -> MacroHarness:
    """Make a new harness for each test. A harness collects log messages, and it changes
    the dbt context while a macro runs. A new harness only reads the macro files. dbt
    keeps the compiled templates in a global cache, thus each test does not compile
    them again."""
    return MacroHarness()
