from unittest import TestCase

import pandas as pd

from pyutil.engine.symbol import Symbol, assets, reference, from_asset, asset
from test.config import connect, test_asset
import pandas.util.testing as pdt

class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()
        from_asset(asset=test_asset(name="A")).save()
        from_asset(asset=test_asset(name="B")).save()


        # Create a text-based post
        #sym1 = Symbol(name="XYZ", properties={"A": 5})
        #sym1.save()

        #sym2 = Symbol(name="aaa", properties={"A": 2, "C": "hah"})
        #sym2.save()

    @classmethod
    def tearDownClass(cls):
        Symbol.drop_collection()

    def test_count(self):
        assert Symbol.objects.count()==2


    def test_empty(self):
        s = Symbol.objects(name="A")[0]
        with self.assertWarns(Warning):
            s.update_ts(name="test", ts=pd.Series())

    def test_update(self):
        s = Symbol.objects(name="A")[0]
        s = s.update_ts(name="test", ts=pd.Series(index=["20100101","20150502"], data=5.0))
        pdt.assert_series_equal(s.asset.time_series["test"].dropna(), pd.Series(index=[pd.Timestamp("20100101"),pd.Timestamp("20150502")], data=5.0, name="test"))

    def test_assets(self):
        x = assets()
        self.assertEquals(len(x), 2)

        x = assets(names=["A"])
        self.assertEquals(len(x), 1)

    def test_reference(self):
        x = reference()
        print(x)
        self.assertEquals(x["group"]["A"],"Equity")

        x = reference(names=["A"])
        print(x)
        self.assertEquals(x["group"]["A"],"Equity")

    def test_unknown(self):
        with self.assertRaises(Exception):
            asset(name="XYZ")

    def test_last(self):
        s = Symbol.objects(name="B")[0]
        self.assertEquals(s.last(name="PX_LAST"), pd.Timestamp("20150422"))



