from fugue.exceptions import FugueWorkflowCompileError


class FugueSQLError(FugueWorkflowCompileError):
    """Fugue SQL error"""


class FugueSQLSyntaxError(FugueSQLError):
    """Fugue SQL syntax error"""
