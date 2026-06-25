from tjf.core.error import TjfError


class RuntimeError(TjfError):
    pass


class NotFoundInRuntime(RuntimeError):
    pass


class AlreadyExistsInRuntime(RuntimeError):
    pass
