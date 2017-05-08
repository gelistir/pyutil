from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets, from_csv
from test.config import read_frame, resource

prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")


class TestAssets(TestCase):
    def test_assets_add(self):
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets = Assets([asset])

        self.assertSetEqual(set(assets.names), {"Peter Maffay"})
        pdt.assert_frame_equal(assets["Peter Maffay"].time_series, prices)
        pdt.assert_series_equal(assets["Peter Maffay"].reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))

        self.assertEquals(len(assets), 1)

        pdt.assert_frame_equal(assets.reference, pd.DataFrame(index=["Peter Maffay"], columns=["a", "b"], data=[[2.0, 3.0]]))

        self.assertEquals(str(assets), "Asset Peter Maffay with series ['A', 'B', 'C', 'D', 'E', 'F', 'G'] and reference [('a', 2.0), ('b', 3.0)]")

    def test_asset(self):
        asset = Asset(name="a", data=pd.DataFrame(index=[0,1], columns=["price"], data=[[2],[1]]), b=3.0, a=2.0)
        self.assertEquals(asset.name, "a")
        pdt.assert_series_equal(asset.reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))
        self.assertListEqual(list(asset.time_series.keys()), ["price"])

    def test_truncate(self):
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets = Assets([asset])
        a = assets.apply(f=lambda x: x.truncate(before=pd.Timestamp("2015-01-01"))).history["B"].dropna()
        self.assertEquals(a.index[0], pd.Timestamp("2015-01-02"))

    def test_sub(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets = Assets([asset_a, asset_b])
        assets_sub = assets.sub(names=["Falco"])
        print(assets_sub)

    def test_tail(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets = Assets([asset_a, asset_b])
        xxx = assets.tail(5)
        pdt.assert_frame_equal(xxx["Peter Maffay"].time_series, prices.tail(5))
        pdt.assert_series_equal(xxx["Peter Maffay"].reference, pd.Series({"b":3.0, "a": 2.0}))

    def test_csv(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)

        x = from_csv(file=resource("assets_ts.csv"), ref_file=resource("assets_ref.csv"))
        self.assertTrue(x["Peter Maffay"] == asset_a)
        self.assertTrue(x["Falco"] == asset_b)

    def test_equals(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets_1 = Assets([asset_a, asset_b])

        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets_2 = Assets([asset_a, asset_b])

        self.assertTrue(assets_1 == assets_2)
        self.assertFalse(assets_1 != assets_2)

    def test_reference_mapping(self):
        x = Assets.map_dict()
        self.assertTrue("CHG_PCT_1D" in x.keys())
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets = Assets([asset_a, asset_b])
        xx = assets.reference_mapping(keys=["a","b"])
        self.assertEquals(xx["b"]["Falco"],4.0)

