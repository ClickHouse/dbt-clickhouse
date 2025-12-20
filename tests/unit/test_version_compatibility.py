from importlib.metadata import version

from packaging.version import Version


def test_version_compatibility():
    """Check our version is never newer than dbt's version"""
    # Extract both versions
    dbt_version_str = version("dbt-core")
    from dbt.adapters.clickhouse import __version__

    clickhouse_version_str = __version__.version

    # Parse versions
    dbt_version = Version(dbt_version_str)
    clickhouse_version = Version(clickhouse_version_str)

    # The dbt-clickhouse version should be smaller than or equal to the installed dbt version
    assert clickhouse_version <= dbt_version, (
        f"Invalid metadata in pyproject.toml: package_version={clickhouse_version_str} is greater than "
        f"dbt version. The adapter version should indicate the minimum supported "
        f"dbt version. Current installed dbt-core version is {dbt_version_str}."
    )
