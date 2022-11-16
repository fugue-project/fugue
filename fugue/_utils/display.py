from typing import Any, Iterable, List

from triad import Schema


class PrettyTable:
    def __init__(
        self,  # noqa: C901
        schema: Schema,
        data: List[Any],
        best_width: int,
        truncate_width: int = 500,
    ):
        raw: List[List[str]] = []
        self.titles = str(schema).split(",")
        col_width_min = [len(t) for t in self.titles]
        col_width_max = list(col_width_min)
        self.col_width = list(col_width_min)
        # Convert all cells to string with truncation
        for row in data:
            raw_row: List[str] = []
            for i in range(len(schema)):
                d = self._cell_to_raw_str(row[i], truncate_width)
                col_width_max[i] = max(col_width_max[i], len(d))
                raw_row.append(d)
            raw.append(raw_row)
        # Adjust col width based on best_width
        # It find the remaining width after fill all cols with min widths,
        # and redistribute the remaining based on the diff between max and min widths
        dt = sorted(
            filter(  # noqa: C407
                lambda x: x[0] > 0,
                [(w - col_width_min[i], i) for i, w in enumerate(col_width_max)],
            )
        )
        if len(dt) > 0:
            remaining = max(0, best_width - sum(col_width_min) - len(col_width_min)) + 1
            total = sum(x[0] for x in dt)
            for diff, index in dt:
                if remaining <= 0:  # pragma: no cover
                    break
                given = remaining * diff // total
                remaining -= given
                self.col_width[index] += given
        # construct data -> List[List[List[str]]], make sure on the same row, each cell
        # has the same length of strings
        self.data = [
            [self._wrap(row[i], self.col_width[i]) for i in range(len(schema))]
            for row in raw
        ]
        blank = ["".ljust(self.col_width[i]) for i in range(len(schema))]
        for row in self.data:
            max_h = max(len(c) for c in row)
            for i in range(len(schema)):
                row[i] += [blank[i]] * (max_h - len(row[i]))

    def to_string(self) -> Iterable[str]:
        yield "|".join(
            self.titles[i].ljust(self.col_width[i]) for i in range(len(self.titles))
        )
        yield "+".join(
            "".ljust(self.col_width[i], "-") for i in range(len(self.titles))
        )
        for row in self.data:
            for tp in zip(*row):
                yield "|".join(tp)

    def _cell_to_raw_str(self, obj: Any, truncate_width: int) -> str:
        raw = "NULL" if obj is None else str(obj)
        if len(raw) > truncate_width:
            raw = raw[: max(0, truncate_width - 3)] + "..."
        return raw

    def _wrap(self, s: str, width: int) -> List[str]:
        res: List[str] = []
        start = 0
        while start < len(s):
            end = min(len(s), start + width)
            sub = s[start:end]
            if end < len(s):
                res.append(sub)
            else:
                res.append(sub.ljust(width, " "))
                break
            start += width
        return res
