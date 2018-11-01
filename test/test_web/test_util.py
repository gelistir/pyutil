import unittest

import pandas as pd
import pandas.util.testing as pdt

# first import settings as we define environment variables in there...
from pyutil.performance.summary import fromNav
from pyutil.web.util import performance
from test.config import read_frame


class TestUtil(unittest.TestCase):
    def test_performance(self):
        prices = read_frame("price.csv")["A"]
        x = performance(series=prices)
        pdt.assert_series_equal(x, fromNav(prices).summary())

    def test_performance_empty(self):
        prices = pd.Series({})
        x = performance(series=prices)
        self.assertTrue(x.empty)

