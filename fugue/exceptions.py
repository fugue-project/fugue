from typing import Any


class FugueError(Exception):
    """Fugue exceptions"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueBug(FugueError):
    """Fugue internal bug"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueDataFrameError(FugueError):
    """Fugue dataframe related error"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueDataFrameInitError(FugueDataFrameError):
    """Fugue dataframe initialization error"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueDataFrameEmptyError(FugueDataFrameError):
    """Fugue dataframe is empty"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueDataFrameOperationError(FugueDataFrameError):
    """Fugue dataframe invalid operation"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueWorkflowError(FugueError):
    """Fugue workflow exceptions"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueWorkflowCompileError(FugueWorkflowError):
    """Fugue workflow compile time error"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueWorkflowCompileValidationError(FugueWorkflowCompileError):
    """Fugue workflow compile time validation error"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueInterfacelessError(FugueWorkflowCompileError):
    """Fugue interfaceless exceptions"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueWorkflowRuntimeError(FugueWorkflowError):
    """Fugue workflow compile time error"""

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueWorkflowRuntimeValidationError(FugueWorkflowRuntimeError):
    """Fugue workflow runtime validation error"""

    def __init__(self, *args: Any):
        super().__init__(*args)
