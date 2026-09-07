import os
import time
from typing import Callable, TypeVar

from dbt.adapters.clickhouse.logger import logger
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

T = TypeVar('T')


def retry_on_database_error(
    fn: Callable[[], T], description: str, attempts: int, delay: float = 0.5
) -> T:
    """Call `fn`, retrying up to `attempts` total tries when it raises
    DbtDatabaseError, sleeping `delay` seconds between tries. Retried failures
    are logged with `description`; the exception from the final attempt
    propagates to the caller. Only use for idempotent operations."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except DbtDatabaseError as ex:
            if attempt >= attempts:
                raise
            logger.warning(f'{description} failed (attempt {attempt}/{attempts}), retrying: {ex}')
            time.sleep(delay)


def compare_versions(v1: str, v2: str) -> int:
    v1_parts = v1.split('.')
    v2_parts = v2.split('.')
    for part1, part2 in zip(v1_parts, v2_parts, strict=False):
        try:
            if int(part1) != int(part2):
                return 1 if int(part1) > int(part2) else -1
        except ValueError as err:
            raise DbtRuntimeError("Version must consist of only numbers separated by '.'") from err
    return 0


def hide_stack_trace(ex: Exception) -> str:

    if not os.getenv("HIDE_STACK_TRACE", ''):
        return str(ex).strip()

    err_msg = str(ex).split("Stack trace")[0].strip()
    return err_msg


def engine_can_atomic_exchange(engine: str) -> bool:
    return engine in ['Atomic', 'Replicated', 'Shared']
