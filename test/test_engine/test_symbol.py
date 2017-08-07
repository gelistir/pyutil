from unittest import TestCase

import pandas as pd

from pyutil.engine.aux import frame2dict
from pyutil.engine.symbol import Symbol, assets, reference, asset, symbol, frame, bulk_update_ref, bulk_update_ts
from test.config import connect, test_asset, read_frame
import pandas.util.testing as pdt

class TestSymbol(TestCase):

    @classmethod
    def setUpClass(cls):
        # connect to test database
        connect()

        asset = test_asset(name="A")

        symbol(name="A", upsert=True).update(properties=asset.reference.to_dict(), timeseries=frame2dict(asset.time_series))

        asset = test_asset(name="B")
        symbol(name="B", upsert=True).update(properties=asset.reference.to_dict(), timeseries=frame2dict(asset.time_series))

        #asset = test_asset(name="B")
        symbol(name="C", upsert=True).update(properties=asset.reference.to_dict())

    @classmethod
    def tearDownClass(cls):
        Symbol.drop_collection()

    def test_count(self):
        assert Symbol.objects.count()==3

    def test_update_ts_warning(self):
        s = symbol(name="A")
        with self.assertWarns(Warning):
            s.update_ts(name="test", ts=pd.Series())

    def test_update_ts(self):
        s = symbol(name="A")
        s = s.update_ts(name="test", ts=pd.Series(index=["20100101","20150502"], data=5.0))
        pdt.assert_series_equal(s.asset.time_series["test"].dropna(), pd.Series(index=[pd.Timestamp("20100101"),pd.Timestamp("20150502")], data=5.0, name="test"))

    def test_update_ref(self):
        s = symbol(name="A")
        s = s.update_ref({"YY":100})
        assert "NAME" in s.properties.keys()
        assert "YY" in s.properties.keys()

    def test_assets(self):
        x = assets()
        self.assertEquals(x.len(), 3)

        x = assets(names=["A"])
        self.assertEquals(x.len(), 1)

    def test_reference(self):
        x = reference()
        self.assertEquals(x["group"]["A"], "Equity")

        x = reference(names=["A"])
        self.assertEquals(x["group"]["A"], "Equity")

    def test_unknown(self):
        # should raise an error as symbol does not exist and upsert not set to true to create it (on the fly)
        with self.assertRaises(Exception):
            asset(name="XYZ")

    def test_frame(self):
        f = frame(timeseries="PX_LAST")
        self.assertSetEqual(set(f.keys()),{"A","B"})


    def test_bulk_ref(self):
        frame = pd.DataFrame(index=["A","B","C"], columns=["VOLATILITY_20D"], data=[[20.0],[30.0],[40.0]])
        bulk_update_ref(frame=frame)

        self.assertEquals(symbol(name="A").properties["VOLATILITY_20D"], 20.0)
        self.assertEquals(symbol(name="B").properties["VOLATILITY_20D"], 30.0)
        self.assertEquals(symbol(name="C").properties["VOLATILITY_20D"], 40.0)

    def test_bulk_ts(self):
        frame = read_frame("price.csv")[["A","B","C"]]
        frame.index = [a.strftime("%Y%m%d") for a in frame.index]
        bulk_update_ts(frame=frame, field="PX_LAST")



