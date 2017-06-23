from unittest import TestCase

import pandas as pd

from pyutil.futures.timeseries import Timeseries

# An empty database requires special care, we do that here...
from test.config import connect
import pandas.util.testing as pdt

class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()

    def test_init(self):
        ts = Timeseries(name="A")
        self.assertEquals(ts.name,"A")

    def test_ts(self):
        ts = Timeseries(name="A").save()
        ts = ts.update_ts(name="PX_LAST", ts=pd.Series(index=["20100101","20110101"], data=[2.0, 3.0]))
        pdt.assert_series_equal(ts.ts["PX_LAST"], pd.Series(index=[pd.Timestamp("2010-01-01"), pd.Timestamp("2011-01-01")], data=[2.0, 3.0], name="PX_LAST"))




