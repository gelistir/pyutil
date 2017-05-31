from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset
from test.config import read_frame


prices = read_frame("price.csv", parse_dates=True)

class TestAsset(TestCase):
    def test_asset(self):
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        self.assertEquals(asset.name, "Peter Maffay")
        self.assertDictEqual(asset.reference.to_dict(), {"b":3.0, "a": 2.0})

        pdt.assert_frame_equal(asset.time_series, prices)

    def test_equals(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)

        self.assertTrue(asset_a == asset_b)
        self.assertFalse(asset_a != asset_b)

        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Peter Maffay", data=2*prices, b=3.0, a=2.0)

        self.assertTrue(asset_a != asset_b)
        self.assertFalse(asset_a == asset_b)

    def test_double_index(self):
        with self.assertRaises(AssertionError):
            Asset(name="Peter", data=pd.Series(index=[1,1,1], data=[1,2,3]))

    def test_inv_index(self):
        with self.assertRaises(AssertionError):
            Asset(name="Peter", data=pd.Series(index=[2,3,1], data=[1,2,3]))

    def test_series_to_frame(self):
        asset = Asset(name="Peter", data=pd.Series(index=[1,2,3], data=[1,2,3]))
        pdt.assert_series_equal(asset.time_series["PX_LAST"], pd.Series(name="PX_LAST", index=[1,2,3], data=[1,2,3]))

    def test_set(self):
        asset = Asset(name="Peter", data=pd.Series(index=[1,2,3], data=[1,2,3]))
        asset["weight"] = pd.Series(index=[1,2],data=10)

        with self.assertRaises(AssertionError):
            asset["weight"] = pd.Series(index=[1,2, 4   ],data=10)

    def test_link(self):
        asset = Asset(name="Peter Maffay Equity", data=prices)
        self.assertEquals(asset.link, "<a href=http://www.bloomberg.com/quote/Peter:Maffay>Peter Maffay Equity</a>")
