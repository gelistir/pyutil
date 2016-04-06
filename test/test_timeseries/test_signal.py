from pyutil.timeseries.signal import oscillator, trend
from unittest import TestCase
import pandas as pd

from test.config import read_series
s = read_series("ts.csv")

index = pd.Timestamp("2015-04-14")


class TestSignal(TestCase):
    def test_oscillator(self):
        self.assertAlmostEqual(oscillator(s)[index], 2.9784281191572063e-05, places=5)

    def test_trend(self):
        self.assertAlmostEqual(trend(s)[index], -0.026098552008347351, places=5)