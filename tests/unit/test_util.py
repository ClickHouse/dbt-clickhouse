from unittest.mock import patch

from dbt.adapters.clickhouse.util import compare_versions, hide_stack_trace


def test_is_before_version():
    assert compare_versions('20.0.0', '21.0.0') == -1
    assert compare_versions('20.1.0', '21.0.0') == -1
    assert compare_versions('20.1.1', '21.0.0') == -1
    assert compare_versions('20.0.0', '21.0') == -1
    assert compare_versions('21.0.0', '21.0.0') == 0
    assert compare_versions('21.1.0', '21.0.0') == 1
    assert compare_versions('22.0.0', '21.0.0') == 1
    assert compare_versions('21.0.1', '21.0.0') == 1
    assert compare_versions('21.0.1', '21.0') == 0


def test_hide_stack_trace_no_env_var():
    # Test when HIDE_STACK_TRACE is not set
    with patch('os.getenv', return_value=''):
        exception = Exception("Error occurred\nStack trace details follow...")
        result = hide_stack_trace(exception)
        assert result == "Error occurred\nStack trace details follow..."


def test_hide_stack_trace_env_var_set():
    # Test when HIDE_STACK_TRACE is set
    with patch('os.getenv', return_value='1'):
        exception = Exception("Error occurred\nStack trace details follow...")
        result = hide_stack_trace(exception)
        assert result == "Error occurred"
