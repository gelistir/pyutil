from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset
from test.config import read_frame


prices = read_frame("price.csv", parse_dates=True)

class TestAsset(TestCase):
    def test_asset(self):
        asset = Asset(name="Peter Maffay", data=prices, group="A", internal="AA", link="www.maffay.com", b=3.0, a=2.0)
        self.assertEqual(asset.name, "Peter Maffay")
        self.assertDictEqual(asset.reference.to_dict(), {"b":3.0, "a": 2.0})
        self.assertEqual(asset.group, "A")
        self.assertEqual(asset.internal, "AA")
        self.assertEqual(asset.link, "www.maffay.com")
        pdt.assert_frame_equal(asset.time_series, prices)

    def test_double_index(self):
        with self.assertRaises(AssertionError):
            Asset(name="Peter", data=pd.Series(index=[1,1,1], data=[1,2,3]))

    def test_inv_index(self):
        with self.assertRaises(AssertionError):
            Asset(name="Peter", data=pd.Series(index=[2,3,1], data=[1,2,3]))

    def test_series_to_frame(self):
        asset = Asset(name="Peter", data=pd.Series(index=[1,2,3], data=[1,2,3]))
        pdt.assert_series_equal(asset.time_series["PX_LAST"], pd.Series(name="PX_LAST", index=[1,2,3], data=[1,2,3]))

