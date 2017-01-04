from pyutil.timeseries.signal import oscillator, trend
from unittest import TestCase
import pandas as pd

from test.config import read_series
s = read_series("ts.csv")

index = pd.Timestamp("2015-04-14")


class TestSignal(TestCase):
    def test_oscillator(self):
        self.assertAlmostEqual(oscillator(s)[index], -1.4304273111303621e-05, places=5)

    def test_trend(self):
        self.assertAlmostEqual(trend(s)[index], -0.06181926927450359, places=5)
