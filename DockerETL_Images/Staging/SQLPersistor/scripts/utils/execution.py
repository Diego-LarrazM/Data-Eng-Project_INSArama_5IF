from typing import TypeAlias
from functools import wraps

ExitCode: TypeAlias = bool

SUCCESS: ExitCode = False  # 0 → success
FAILURE: ExitCode = True   # 1 → failure

def safe_execute(_func=None, *, fail_return=FAILURE):
    def decorator(operation):
        @wraps(operation)
        def wrapper(*args, **kwargs):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                print(f"An error occurred during execution: {e}")
                return fail_return
        return wrapper

    # if used without parentheses: @safe_execute
    if _func is not None:
        return decorator(_func)

    # if used with parentheses: @safe_execute(fail_return=...)
    return decorator
