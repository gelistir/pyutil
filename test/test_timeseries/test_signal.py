from pyutil.timeseries.signal import trend
from unittest import TestCase
import pandas as pd

from pyutil.test.aux import read_series
from test.config import resource

s = read_series(resource("ts.csv"))


index = pd.Timestamp("2015-04-14")


class TestSignal(TestCase):
    def test_trend(self):
        self.assertAlmostEqual(trend(s)[index], -0.06181926927450359, places=5)
