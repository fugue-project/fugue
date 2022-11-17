# pylint: disable-all
# flake8: noqa

from datetime import date, datetime
from typing import Any
from unittest import TestCase
import copy
import numpy as np
import pandas as pd
from fugue.bag import Bag, LocalBag
from fugue.exceptions import FugueDataFrameOperationError, FugueDatasetEmptyError
from pytest import raises
from triad.collections.schema import Schema


class BagTests(object):
    """DataFrame level general test suite.
    All new DataFrame types should pass this test suite.
    """

    class Tests(TestCase):
        @classmethod
        def setUpClass(cls):
            pass

        @classmethod
        def tearDownClass(cls):
            pass

        def bg(self, data: Any = None) -> Bag:  # pragma: no cover
            raise NotImplementedError

        def test_init_basic(self):
            raises(Exception, lambda: self.bg())
            bg = self.bg([])
            assert bg.empty
            assert copy.copy(bg) is bg
            assert copy.deepcopy(bg) is bg

        def test_peek(self):
            bg = self.bg([])
            raises(FugueDatasetEmptyError, lambda: bg.peek())

            bg = self.bg(["x"])
            assert not bg.is_bounded or 1 == bg.count()
            assert not bg.empty
            assert "x" == bg.peek()

        def test_as_array(self):
            bg = self.bg([2, 1, "a"])
            assert set([1, 2, "a"]) == set(bg.as_array())

        def test_as_array_special_values(self):
            bg = self.bg([2, None, "a"])
            assert set([None, 2, "a"]) == set(bg.as_array())

            bg = self.bg([np.float16(0.1)])
            assert set([np.float16(0.1)]) == set(bg.as_array())

        def test_head(self):
            bg = self.bg([])
            assert [] == bg.head(0).as_array()
            assert [] == bg.head(1).as_array()
            bg = self.bg([["a", 1]])
            if bg.is_bounded:
                assert [["a", 1]] == bg.head(1).as_array()
            assert [] == bg.head(0).as_array()

            bg = self.bg([1, 2, 3, 4])
            assert 2 == bg.head(2).count()
            bg = self.bg([1, 2, 3, 4])
            assert 4 == bg.head(10).count()
            h = bg.head(10)
            assert h.is_local and h.is_bounded

        def test_show(self):
            bg = self.bg(["a", 1])
            bg.show()
            bg.show(n=0)
            bg.show(n=1)
            bg.show(n=2)
            bg.show(title="title")
            bg.metadata["m"] = 1
            bg.show()
