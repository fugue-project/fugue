from fugue.exceptions import FugueWorkflowCompileError, FugueWorkflowRuntimeError


class FugueSQLError(FugueWorkflowCompileError):
    """Fugue SQL error"""


class FugueSQLSyntaxError(FugueSQLError):
    """Fugue SQL syntax error"""


class FugueSQLRuntimeError(FugueWorkflowRuntimeError):
    """Fugue SQL runtime error"""
