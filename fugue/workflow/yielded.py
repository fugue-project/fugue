class Yielded(object):
    def __init__(self, path: str):
        self._path = path

    @property
    def path(self) -> str:
        return self._path
