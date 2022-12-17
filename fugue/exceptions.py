class FugueError(Exception):
    """Fugue exceptions"""


class FugueBug(FugueError):
    """Fugue internal bug"""


class FugueInvalidOperation(FugueError):
    """Invalid operation on the Fugue framework"""


class FuguePluginsRegistrationError(FugueError):
    """Fugue plugins registration error"""


class FugueDataFrameError(FugueError):
    """Fugue dataframe related error"""


class FugueDataFrameInitError(FugueDataFrameError):
    """Fugue dataframe initialization error"""


class FugueDatasetEmptyError(FugueDataFrameError):
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


class FugueSQLError(FugueWorkflowCompileError):
    """Fugue SQL error"""


class FugueSQLSyntaxError(FugueSQLError):
    """Fugue SQL syntax error"""


class FugueSQLRuntimeError(FugueWorkflowRuntimeError):
    """Fugue SQL runtime error"""
