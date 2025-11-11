from typing import TypeAlias
from functools import wraps

ExitCode: TypeAlias = bool

SUCCESS: ExitCode = False  # 0 → success
FAILURE: ExitCode = True   # 1 → failure


def safe_execute(operation):
    @wraps(operation)
    def wrapper() -> ExitCode:
        try:
            return operation()
        except Exception as e:
            print(f"An error occurred during execution: {e}")
            return FAILURE
    return wrapper