import pytest
from dbt_common.exceptions import CompilationError

from tests.unit.macro_harness import MacroHarness, SandboxSafeMock

# the harness tests use these synthetic macros, so a test fails only
# when the harness breaks, not when somebody edits a macro in this package
SYNTHETIC_MACROS = """
{% macro emits_sql(relation_name) %}
  select * from {{ relation_name }}
{% endmacro %}

{% macro returns_mapping() %}
  {{ return({'mv1': 'select 1'}) }}
{% endmacro %}

{% macro calls_returns_mapping() %}
  {{ return(returns_mapping()) }}
{% endmacro %}

{% macro reads_adapter() %}
  {{ log('asked the adapter for the cluster') }}
  {{ return(adapter.get_clickhouse_cluster_name()) }}
{% endmacro %}
"""


@pytest.fixture
def harness(tmp_path):
    (tmp_path / 'synthetic.sql').write_text(SYNTHETIC_MACROS)
    return MacroHarness(tmp_path)


class TestMacroHarness:
    def test_returns_rendered_text(self, harness):
        assert harness.call('emits_sql', 'people').strip() == 'select * from people'

    def test_returns_value_from_return(self, harness):
        assert harness.call('returns_mapping') == {'mv1': 'select 1'}

    def test_macro_calls_another_macro(self, harness):
        assert harness.call('calls_returns_mapping') == {'mv1': 'select 1'}

    def test_injected_context_visible_to_macro(self, harness):
        adapter = SandboxSafeMock()
        adapter.get_clickhouse_cluster_name.return_value = '"test_shard"'

        assert harness.call('reads_adapter', context={'adapter': adapter}) == '"test_shard"'

    def test_injected_context_does_not_leak_into_later_calls(self, harness):
        adapter = SandboxSafeMock()
        adapter.get_clickhouse_cluster_name.return_value = '"test_shard"'
        harness.call('reads_adapter', context={'adapter': adapter})

        with pytest.raises(CompilationError, match='adapter'):
            harness.call('reads_adapter')

    def test_collects_macro_log_messages(self, harness):
        harness.call('reads_adapter', context={'adapter': SandboxSafeMock()})

        assert harness.logs == ['asked the adapter for the cluster']

    def test_unknown_macro_raises_key_error(self, harness):
        with pytest.raises(KeyError, match='no_such_macro'):
            harness.call('no_such_macro')

    def test_duplicate_macro_name_raises(self, tmp_path):
        (tmp_path / 'a.sql').write_text('{% macro shared() %}a{% endmacro %}')
        (tmp_path / 'b.sql').write_text('{% macro shared() %}b{% endmacro %}')

        with pytest.raises(ValueError, match='shared is defined twice'):
            MacroHarness(tmp_path)


class TestClickHouseMacros:
    def test_all_macros_compile(self, macros):
        """Every macro file this package ships in ``dbt/include/clickhouse/macros`` must
        parse, and every name the extractor finds must exist in the compiled file. A
        duplicate name fails earlier, in the constructor."""
        assert macros.macro_names, 'no macros found; is MACROS_ROOT correct?'

        for name in macros.macro_names:
            macros.compile(name)
