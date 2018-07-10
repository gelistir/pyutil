# from unittest import TestCase
#
# import pandas as pd
# import pandas.util.testing as pdt
#
# from pyutil.sql.model.ts import Timeseries
# from test.test_sql.product import Product
#
#
# class TestTimeseries(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         cls.p1 = Product(name="A")
#         cls.p2 = Product(name="B")
#         cls.p3 = Product(name="C")
#
#     def test_timeseries(self):
#         t0 = pd.Timestamp("1978-11-12")
#         t1 = pd.Timestamp("1978-11-13")
#
#         ts1 = Timeseries(name="x", product=self.p1, data={t0: 10.1}, secondary=self.p2)
#         ts2 = Timeseries(name="y", product=self.p1, data={t1: 11.1})
#         ts3 = Timeseries(name="z", product=self.p1)
#         ts4 = Timeseries(name="a", product=self.p1, secondary=self.p2, tertiary=self.p3, data={t1: 11.1})
#
#         self.assertIsNotNone(ts1.secondary)
#         self.assertIsNone(ts2.secondary)
#         self.assertIsNone(ts2.tertiary)
#         self.assertIsNotNone(ts4.tertiary)
#
#         pdt.assert_series_equal(ts4.series, pd.Series({t1: 11.1}))
#         pdt.assert_series_equal(ts2.series, pd.Series({t1: 11.1}))
#         pdt.assert_series_equal(ts1.series, pd.Series({t0: 10.1}))
#
#         self.assertEqual(ts1.key, ("x", self.p2))
#         self.assertEqual(ts2.key, "y")
#         self.assertEqual(ts4.key, ("a", self.p2, self.p3))
#
#         pdt.assert_frame_equal(self.p1.frame("x"),
#                                pd.DataFrame(index=[t0], columns=[self.p2], data=[[10.1]]))
#
#         x = ts1.upsert(ts={t0: 11.1, t1: 12.1})
#         pdt.assert_series_equal(x.series, pd.Series({t0: 11.1, t1: 12.1}))
#
#     def test_upsert(self):
#         ts1 = Timeseries(name="x", product=self.p1)
#
#         # You can use date or not (as you wish), and you can overwrite existing data...
#         ts1.upsert(ts={pd.Timestamp("2010-01-01").date(): 1.1})
#         ts1.upsert(ts={pd.Timestamp("2010-01-02"): 3.1})
#
#         ts1.upsert(ts={pd.Timestamp("2010-01-02").date(): 8.1})
#         ts1.upsert(ts={pd.Timestamp("2010-01-01"): 10.1})
#
#         x = ts1.series
#         self.assertIsInstance(x.index[0], pd.Timestamp)
#         self.assertFalse(x.index.has_duplicates)
#         self.assertTrue(x.index.is_monotonic_increasing)
#
#         pdt.assert_series_equal(x, pd.Series({pd.Timestamp("2010-01-01"): 10.1, pd.Timestamp("2010-01-02"): 8.1}))
#
#     def test_with_nan(self):
#         import numpy as np
#
#         ts1 = Timeseries(name="x", product=self.p1)
#
#         ts1.upsert(ts={pd.Timestamp("2010-01-01"): np.nan})
#         pdt.assert_series_equal(ts1.series, pd.Series({}))
#
#         ts1.upsert(ts={pd.Timestamp("2010-01-01"): 5.0})
#         pdt.assert_series_equal(ts1.series, pd.Series({pd.Timestamp("2010-01-01"): 5.0}))
#
#     def test_wrong_index(self):
#         with self.assertRaises(AssertionError):
#             Timeseries(name="peter", product=self.p1, data={"A": 2.0})

