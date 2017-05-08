from unittest import TestCase

import pandas as pd

from pyutil.engine.symbol import Symbol, assets
from pyutil.mongo.asset import Asset
from test.config import connect
import pandas.util.testing as pdt

class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()

        # Create a text-based post
        sym1 = Symbol(name="XYZ", internal="XYZ internal", group="A")
        sym1.save()

        sym2 = Symbol(name="aaa", internal="aaa internal", group="B")
        sym2.save()

    @classmethod
    def tearDownClass(cls):
        Symbol.drop_collection()

    def test_count(self):
        assert Symbol.objects.count()==2


    def test_empty(self):
        s = Symbol.objects(name="aaa")[0]
        with self.assertWarns(Warning):
            s.update_ts(name="test", ts=pd.Series())

    def test_update(self):
        s = Symbol.objects(name="aaa")[0]
        s = s.update_ts(name="test", ts=pd.Series(index=["20100101","20150502"], data=5.0))
        pdt.assert_series_equal(s.asset.time_series["test"], pd.Series(index=[pd.Timestamp("20100101"),pd.Timestamp("20150502")], data=5.0, name="test"))

    def test_assets(self):
        x = assets()
        self.assertEquals(len(x), 2)

        x = assets(names=["aaa"])
        self.assertEquals(len(x), 1)

