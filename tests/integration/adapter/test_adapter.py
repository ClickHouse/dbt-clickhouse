from dbt.adapters.clickhouse.impl import compare_versions


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
