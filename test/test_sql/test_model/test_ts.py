from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.model.ts import Timeseries
from test.test_sql.product import Product


class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

    def test_timeseries(self):
        ts1 = Timeseries(name="x", product=self.p1, data={pd.Timestamp("12-11-1978"): 10.0}, secondary=self.p2)
        ts2 = Timeseries(name="y", product=self.p1, data={pd.Timestamp("13-11-1978"): 11.0})
        ts3 = Timeseries(name="z", product=self.p1)

        self.assertIsNotNone(ts1.secondary)
        self.assertIsNone(ts2.secondary)

        pdt.assert_series_equal(ts2.series, pd.Series({pd.Timestamp("13-11-1978"): 11.0}))
        pdt.assert_series_equal(ts1.series, pd.Series({pd.Timestamp("12-11-1978"): 10.0}))

        self.assertEqual(ts1.key, ("x", self.p2))
        self.assertEqual(ts2.key, "y")

        pdt.assert_frame_equal(self.p1.frame("x"),
                               pd.DataFrame(index=[pd.Timestamp("12-11-1978")], columns=[self.p2], data=[[10.0]]))

        self.assertEqual(ts2.last_valid, pd.Timestamp("13-11-1978"))
        self.assertEqual(ts1.last_valid, pd.Timestamp("12-11-1978"))
        self.assertIsNone(ts3.last_valid)

        x = ts1.upsert(ts={pd.Timestamp("12-11-1978"): 11.0, pd.Timestamp("13-11-1978"): 12.0})
        pdt.assert_series_equal(x.series,
                                pd.Series({pd.Timestamp("12-11-1978"): 11.0, pd.Timestamp("13-11-1978"): 12.0}))

    def test_upsert(self):
        ts1 = Timeseries(name="x", product=self.p1)
        with self.assertRaises(AssertionError):
            ts1.upsert(ts={2: 1.0})

        ts1.upsert(ts={pd.Timestamp("2010-01-01").date(): 1.0})
        ts1.upsert(ts={pd.Timestamp("2010-01-02"): 3.0})

        ts1.upsert(ts={pd.Timestamp("2010-01-02").date(): 8.0})
        ts1.upsert(ts={pd.Timestamp("2010-01-01"): 10.0})

        x = ts1.series
        self.assertIsInstance(x.index[0], pd.Timestamp)
        self.assertFalse(x.index.has_duplicates)
        self.assertTrue(x.index.is_monotonic_increasing)

        pdt.assert_series_equal(x, pd.Series({pd.Timestamp("2010-01-01"): 10.0, pd.Timestamp("2010-01-02"): 8.0}))
