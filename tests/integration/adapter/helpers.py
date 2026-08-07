import os
import time
from typing import Any, Callable, Optional, TypedDict


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


retry_config = TypedDict('retry_config', {'max_retries': int, 'delay': float})
DEFAULT_RETRY_CONFIG: retry_config = {
    "max_retries": 20,
    "delay": 0.5,
}


def retry_until_assertion_passes(
    func: Callable[[], Any],
    max_retries: int = DEFAULT_RETRY_CONFIG["max_retries"],
    delay: float = DEFAULT_RETRY_CONFIG["delay"],
) -> Any:
    last_error: Optional[AssertionError] = None
    for attempt in range(max_retries + 1):  # +1 to include the initial attempt
        try:
            return func()
        except AssertionError as e:
            last_error = e
            if attempt < max_retries:  # Don't sleep after the last attempt
                time.sleep(delay)
            continue
    # If we get here, all retries failed
    if last_error:
        raise last_error
    return None
