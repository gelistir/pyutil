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
        assets = Assets()
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets.add(asset)

        self.assertSetEqual(set(assets.keys()), {"Peter Maffay"})
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

