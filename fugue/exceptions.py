from typing import Any


class FugueError(Exception):
    """Fugue exceptions
    """

    def __init__(self, *args: Any):
        super().__init__(*args)


class FugueInterfacelessError(FugueError):
    """Fugue exceptions
    """

    def __init__(self, *args: Any):
        super().__init__(*args)
