from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.asset import Asset


class TestAsset(TestCase):
    def test_asset(self):
        asset = Asset(name="a", data=pd.DataFrame(index=[0,1], columns=["price"], data=[[2],[1]]), b=3.0, a=2.0)
        self.assertEquals(asset.name, "a")
        pdt.assert_series_equal(asset.reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))
        self.assertListEqual(list(asset.series_names()), ["price"])


