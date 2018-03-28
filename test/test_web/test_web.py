from unittest import TestCase
import pandas as pd
from test.config import read_series
import numpy as np

nav = read_series("nav.csv", parse_dates=True)

from pyutil.web.aux import series2array, frame2dict, int2time, double2percent


class TestWeb(TestCase):
    def test_series2array(self):
        x = series2array(nav)
        self.assertEqual(len(x), 41)
        self.assertEqual(x[0], [1418252400000, 1.2940225007396362])

    def test_frame2dict(self):
        x = nav.to_frame(name="nav")
        self.assertIsInstance(x, pd.DataFrame)
        y = frame2dict(x)

        self.assertSetEqual({"columns", "data"}, set(y.keys()))
        self.assertListEqual(y["columns"], ["nav"])
        for n, a in enumerate(y["data"]):
            self.assertAlmostEqual(a["nav"], x.values[n], places=10)

    def test_date(self):
        x = pd.Series(index=[pd.Timestamp("2020-01-01").date(), pd.Timestamp("2020-01-02").date()], data=[5.0, 6.0])
        y = pd.Series(index=[pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")], data=[5.0, 6.0])

        self.assertEqual(series2array(x)[0][0], 1577833200000)
        self.assertListEqual(series2array(x), series2array(y))
        self.assertEqual(pd.Timestamp(1577833200000 * 1e6, tz="CET").date(), pd.Timestamp("2020-01-01").date())

    def test_frame(self):
        x = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11").date()], data=[[2.0]])
        y = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11")], data=[[2.0]])
        self.assertEqual(x.to_json(), y.to_json())

    def test_int2time(self):
        self.assertEqual(int2time(x=1418252400000), "2014-12-10")
        self.assertEqual(int2time(x=""), "")

    def test_double2percent(self):
        self.assertEqual(double2percent(x=0.20), "20.00%")
        self.assertEqual(double2percent(x=np.nan), "")