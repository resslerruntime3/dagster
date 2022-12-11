import traceback
from types import TracebackType
from typing import Any, NamedTuple, Optional, Sequence, Tuple, Type, Union

import dagster._check as check
from dagster._serdes import whitelist_for_serdes


# mypy does not support recursive types, so "cause" has to be typed `Any`
@whitelist_for_serdes
class SerializableErrorInfo(
    NamedTuple(
        "SerializableErrorInfo",
        [("message", str), ("stack", Sequence[str]), ("cls_name", Optional[str]), ("cause", Any)],
    )
):
    # serdes log
    # * added cause - default to None in constructor to allow loading old entries
    #
    def __new__(
        cls,
        message: str,
        stack: Sequence[str],
        cls_name: Optional[str],
        cause: Optional["SerializableErrorInfo"] = None,
    ):
        return super().__new__(cls, message, stack, cls_name, cause)

    def __str__(self):
        return self.to_string()

    def to_string(self):
        stack_str = "\nStack Trace:\n" + "".join(self.stack) if self.stack else ""
        cause_str = (
            "\nThe above exception was caused by the following exception:\n"
            + self.cause.to_string()
            if self.cause
            else ""
        )
        return "{err.message}{stack}{cause}".format(err=self, stack=stack_str, cause=cause_str)


def _serializable_error_info_from_tb(tb: traceback.TracebackException) -> SerializableErrorInfo:
    return SerializableErrorInfo(
        # usually one entry, multiple lines for SyntaxError
        "".join(list(tb.format_exception_only())),
        tb.stack.format(),
        tb.exc_type.__name__ if tb.exc_type is not None else None,
        _serializable_error_info_from_tb(tb.__cause__) if tb.__cause__ else None,
    )


def serializable_error_info_from_exc_info(
    exc_info: Union[
        Tuple[Type[BaseException], BaseException, TracebackType], Tuple[None, None, None]
    ],
    # Whether to forward serialized errors thrown from subprocesses
    hoist_user_code_error: Optional[bool] = True,
) -> SerializableErrorInfo:
    # `sys.exc_info() return Tuple[None, None, None] when there is no exception being processed. We accept this in
    # the type signature here since this function is meant to directly receive the return value of
    # `sys.exc_info`, but the function should never be called when there is no exception to process.
    exc_type, e, tb = exc_info
    exc_type = check.not_none(exc_type)
    e = check.not_none(e)
    tb = check.not_none(tb)
    from dagster._core.errors import DagsterUserCodeProcessError

    if (
        hoist_user_code_error
        and isinstance(e, DagsterUserCodeProcessError)
        and len(e.user_code_process_error_infos) == 1
    ):
        return e.user_code_process_error_infos[0]
    else:
        tb_exc = traceback.TracebackException(exc_type, e, tb)
        return _serializable_error_info_from_tb(tb_exc)
