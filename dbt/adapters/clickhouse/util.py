import os


def hide_stack_trace(ex: Exception) -> str:

    if not os.getenv("HIDE_STACK_TRACE", ''):
        return str(ex).strip()

    err_msg = str(ex).split("Stack trace")[0].strip()
    return err_msg


def engine_can_atomic_exchange(engine: str) -> bool:
    return engine in ['Atomic', 'Replicated', 'Shared']
