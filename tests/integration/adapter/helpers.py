import os
from typing import Optional


def below_version(major: int, minor: int = 0, _ch_test_version_value: Optional[str] = None) -> bool:
    """
    >>> below_version(25, _ch_test_version_value='24.8')
    True
    >>> below_version(25, _ch_test_version_value='25.7')
    False
    >>> below_version(25, 7, _ch_test_version_value='25.6')
    True
    >>> below_version(25, 7, _ch_test_version_value='25.7')
    False
    """
    current_version = (
        _ch_test_version_value
        or os.environ.get('DBT_CH_TEST_CH_VERSION', '0.0')
        or '0.0'  # Extra 0.0 to make Mypy happy
    )
    actual_major, actual_minor = current_version.split('.')
    return int(actual_major) < major or (int(actual_major) == major and int(actual_minor) < minor)
