from unittest import TestCase

import pandas as pd

from pyutil.engine.aux import frame2dict
from pyutil.engine.symbol import Symbol, assets, reference, asset, symbol
from test.config import connect, test_asset
import pandas.util.testing as pdt

class TestSymbol(TestCase):

    @classmethod
    def setUpClass(cls):
        # connect to test database
        connect()

        asset = test_asset(name="A")
        symbol(name="A").update(properties=asset.reference.to_dict(), timeseries=frame2dict(asset.time_series))

        asset = test_asset(name="B")
        symbol(name="B").update(properties=asset.reference.to_dict(), timeseries=frame2dict(asset.time_series))

    @classmethod
    def tearDownClass(cls):
        Symbol.drop_collection()

    def test_count(self):
        assert Symbol.objects.count()==2


    def test_empty(self):
        s = symbol(name="A")
        with self.assertWarns(Warning):
            s.update_ts(name="test", ts=pd.Series())

    def test_update(self):
        s = symbol(name="A")
        s = s.update_ts(name="test", ts=pd.Series(index=["20100101","20150502"], data=5.0))
        pdt.assert_series_equal(s.asset.time_series["test"].dropna(), pd.Series(index=[pd.Timestamp("20100101"),pd.Timestamp("20150502")], data=5.0, name="test"))

    def test_assets(self):
        x = assets()
        self.assertEquals(x.len(), 2)

        x = assets(names=["A"])
        self.assertEquals(x.len(), 1)

    def test_reference(self):
        x = reference()
        self.assertEquals(x["group"]["A"],"Equity")

        x = reference(names=["A"])
        self.assertEquals(x["group"]["A"],"Equity")

    def test_unknown(self):
        with self.assertRaises(Exception):
            asset(name="XYZ")

    def test_last(self):
        s = symbol(name="B")
        self.assertEquals(s.last(name="PX_LAST"), pd.Timestamp("20150422"))



