from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.products import _ReadDict


class TestReaddict(TestCase):
    def test_dict1(self):
        a = _ReadDict({"a": 1.0, "b": 2.0})
        self.assertIsNone(a["c"])
        self.assertEqual(a["a"], 1.0)
        self.assertEqual(a["b"], 2.0)

        with self.assertRaises(TypeError):
            a["a"] = 3.0

        pdt.assert_series_equal(pd.Series(a), pd.Series({"a": 1.0, "b": 2.0}))

