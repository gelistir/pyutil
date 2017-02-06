from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets, frame2assets
from pyutil.mongo.mongoArchive import MongoArchive
from test.config import read_frame


prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")

class TestAssetsMongo(TestCase):
    @classmethod
    def setUp(self):
        self.archive = MongoArchive()

        assets = frame2assets(prices, symbols)
        for asset in assets.values():
            self.archive.update_asset(asset=asset)

        self.assets = self.archive.assets(names=["A", "B", "C"])


    def test_symbols(self):
        pdt.assert_frame_equal(symbols.ix[["A","B","C"]].sort_index(axis=1), self.assets.reference.sort_index(axis=1), check_dtype=False)

    def test_history(self):
        self.assertSetEqual(set(self.assets.keys()), {"A", "B", "C"})
        pdt.assert_frame_equal(self.assets.frame("PX_LAST").sort_index(axis=1), prices[["A","B","C"]].sort_index(axis=1))


class TestAssets(TestCase):
    def test_assets_add(self):
        assets = Assets()
        asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        assets.add(asset)

        self.assertSetEqual(set(assets.keys()), {"Peter Maffay"})
        pdt.assert_frame_equal(assets["Peter Maffay"].data, prices)
        pdt.assert_series_equal(assets["Peter Maffay"].reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))

        self.assertEquals(len(assets), 1)

        pdt.assert_frame_equal(assets.reference, pd.DataFrame(index=["Peter Maffay"], columns=["a", "b"], data=[[2.0, 3.0]]))

        self.assertEquals(str(assets), "Asset Peter Maffay with series ['A', 'B', 'C', 'D', 'E', 'F', 'G'] and reference [('a', 2.0), ('b', 3.0)]")



