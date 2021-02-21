class FugueError(Exception):
    """Fugue exceptions"""


class FugueBug(FugueError):
    """Fugue internal bug"""


class FugueDataFrameError(FugueError):
    """Fugue dataframe related error"""


class FugueDataFrameInitError(FugueDataFrameError):
    """Fugue dataframe initialization error"""


class FugueDataFrameEmptyError(FugueDataFrameError):
    """Fugue dataframe is empty"""


class FugueDataFrameOperationError(FugueDataFrameError):
    """Fugue dataframe invalid operation"""


class FugueWorkflowError(FugueError):
    """Fugue workflow exceptions"""


class FugueWorkflowCompileError(FugueWorkflowError):
    """Fugue workflow compile time error"""


class FugueWorkflowCompileValidationError(FugueWorkflowCompileError):
    """Fugue workflow compile time validation error"""


class FugueInterfacelessError(FugueWorkflowCompileError):
    """Fugue interfaceless exceptions"""


class FugueWorkflowRuntimeError(FugueWorkflowError):
    """Fugue workflow compile time error"""


class FugueWorkflowRuntimeValidationError(FugueWorkflowRuntimeError):
    """Fugue workflow runtime validation error"""
