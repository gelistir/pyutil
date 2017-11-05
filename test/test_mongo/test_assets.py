from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from test.config import read_frame

prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")


class TestAssets(TestCase):
    def test_assets_add(self):
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets = Assets({asset.name: asset})

        self.assertSetEqual(set(assets.keys()), {"Peter Maffay"})
        pdt.assert_frame_equal(assets["Peter Maffay"].time_series, prices)
        pdt.assert_series_equal(assets["Peter Maffay"].reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))

        self.assertEqual(assets.len(), 1)

        pdt.assert_frame_equal(assets.reference, pd.DataFrame(index=["Peter Maffay"], columns=["a", "b"], data=[[2.0, 3.0]]))

        self.assertEqual(str(assets), "Asset Peter Maffay with series ['A', 'B', 'C', 'D', 'E', 'F', 'G'] and reference [('a', 2.0), ('b', 3.0)]")

    def test_asset(self):
        asset = Asset(name="a", data=pd.DataFrame(index=[0,1], columns=["price"], data=[[2],[1]]), b=3.0, a=2.0)
        self.assertEqual(asset.name, "a")
        pdt.assert_series_equal(asset.reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))
        self.assertListEqual(list(asset.time_series.keys()), ["price"])

    def test_truncate(self):
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets = Assets({"Peter Maffay": asset})
        a = assets.apply(f=lambda x: x.truncate(before=pd.Timestamp("2015-01-01"))).history["B"].dropna()
        self.assertEqual(a.index[0], pd.Timestamp("2015-01-02"))

    def test_sub(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets = Assets({"Peter Maffay": asset_a, "Falco": asset_b})
        assets_sub = assets.sub(names=["Falco"])
        print(assets_sub)

    def test_tail(self):
        asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
        assets = Assets({"Peter Maffay": asset_a, "Falco": asset_b})
        xxx = assets.tail(5)
        pdt.assert_frame_equal(xxx["Peter Maffay"].time_series, prices.tail(5))
        pdt.assert_series_equal(xxx["Peter Maffay"].reference, pd.Series({"b":3.0, "a": 2.0}))

    def test_internal(self):
        peter = Asset(name="Peter Maffay", group="A", data=prices, b=3.0, a=2.0, internal="jaja")
        x = Assets({"Peter Maffay": peter})
        pdt.assert_series_equal(x.internal, pd.Series(index=["Peter Maffay"], data=["jaja"]))
        pdt.assert_series_equal(x.group, pd.Series(index=["Peter Maffay"], data=["A"]))

    def test_empty(self):
        x = Assets({})
        self.assertTrue(x.empty)


        #self.assertTrue(x["Falco"] == asset_b)

    # def test_equals(self):
    #     asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
    #     asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
    #     assets_1 = Assets({"Peter Maffay": asset_a, "Falco": asset_b})
    #
    #
    #     asset_a = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
    #     asset_b = Asset(name="Falco", data=prices, b=4.0, a=2.0)
    #     assets_2 = Assets({"Peter Maffay": asset_a, "Falco": asset_b})
    #
    #     self.assertTrue(assets_1 == assets_2)
    #     self.assertFalse(assets_1 != assets_2)


