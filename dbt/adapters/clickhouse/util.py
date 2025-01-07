import os

from dbt_common.exceptions import DbtRuntimeError


def compare_versions(v1: str, v2: str) -> int:
    v1_parts = v1.split('.')
    v2_parts = v2.split('.')
    for part1, part2 in zip(v1_parts, v2_parts):
        try:
            if int(part1) != int(part2):
                return 1 if int(part1) > int(part2) else -1
        except ValueError:
            raise DbtRuntimeError("Version must consist of only numbers separated by '.'")
    return 0


def hide_stack_trace(ex: Exception) -> str:

    if not os.getenv("HIDE_STACK_TRACE", ''):
        return str(ex).strip()

    err_msg = str(ex).split("Stack trace")[0].strip()
    return err_msg
