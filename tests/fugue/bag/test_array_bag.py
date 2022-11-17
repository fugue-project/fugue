import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from pytest import raises
from triad.collections.schema import Schema

from fugue import ArrayBag, Bag
from fugue.dataframe import ArrayDataFrame, PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_test.bag_suite import BagTests


class ArrayBagTests(BagTests.Tests):
    def bg(self, data: Any = None) -> Bag:
        return ArrayBag(data)

    def test_array_bag_init(self):
        def _it():
            yield from [1, 2, 3]

        bg = self.bg([])
        assert bg.count() == 0
        assert bg.is_local
        assert bg.is_bounded
        assert bg.as_local() is bg
        assert bg.empty
        assert bg.native == []

        for x in [[1, 2, 3], _it(), set([1, 2, 3])]:
            bg = self.bg(x)
            assert bg.count() == 3
            assert bg.is_local
            assert bg.is_bounded
            assert bg.as_local() is bg
            assert not bg.empty
            assert 1 == bg.num_partitions
            assert isinstance(bg.native, list)

        bg = self.bg(x + 1 for x in [])
        assert bg.count() == 0
        bg = self.bg(x + 1 for x in [1, 2, 3])
        assert bg.count() == 3
