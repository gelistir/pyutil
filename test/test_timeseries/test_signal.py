from pyutil.timeseries.signal import trend
from unittest import TestCase
import pandas as pd

from test.config import read_series
s = read_series("ts.csv")

index = pd.Timestamp("2015-04-14")


class TestSignal(TestCase):
    def test_trend(self):
        self.assertAlmostEqual(trend(s)[index], -0.06181926927450359, places=5)
