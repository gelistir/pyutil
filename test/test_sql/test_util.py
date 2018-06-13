import pandas as pd
from unittest import TestCase

from pyutil.sql.util import parse, to_pandas, from_pandas
import pandas.util.testing as pdt

class TestUtil(TestCase):
    def test_parse(self):
        self.assertEqual(parse("2.0", "integer"), 2)
        self.assertEqual(parse("2.0", "float"), 2.0)
        self.assertEqual(parse("2.0", "string"), "2.0")
        self.assertEqual(parse("0.2", "percentage"), 0.2)
        self.assertEqual(parse("1527724800000", "date"), pd.Timestamp("2018-05-31"))

    def test_to_pandas_empty(self):
        pdt.assert_series_equal(to_pandas(x=None), pd.Series({}))

    def test_to_pandas_full(self):
        x = pd.Series({pd.Timestamp("2010-04-23"): 23.2})
        pdt.assert_series_equal(to_pandas(from_pandas(x)), x)

    def test_to_pandas_full_float(self):
        x = pd.Series({pd.Timestamp("2010-04-23"): 23.0})
        pdt.assert_series_equal(to_pandas(from_pandas(x)).apply(float), x)

    def test_none(self):
        self.assertIsNone(parse(x=None))
