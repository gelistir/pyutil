from unittest import TestCase

import pandas as pd

from pyutil.engine.frame import store, load, keys, Frame
from pyutil.mongo.connect import connect_mongo

import pandas.util.testing as pdt


class TestFrame(TestCase):
    @classmethod
    def setUpClass(cls):
        connect_mongo('frames', host="testmongo", alias="default")

        store(name="f1", pandas_object=pd.DataFrame(index=["A", "B"], columns=["X", "Y"], data=[[2, 3], [10, 20]]))
        store(name="f2", pandas_object=pd.Series(index=["A", "B"], data=[1, 2]))

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()


    def test_get_frame(self):
        pdt.assert_frame_equal(load(name="f1").frame,
                               pd.DataFrame(index=["A", "B"], columns=["X", "Y"], data=[[2, 3], [10, 20]]))

    def test_get_series(self):
        pdt.assert_series_equal(load(name="f2").series, pd.Series(index=["A", "B"], data=[1, 2]))

    def test_keys(self):
        self.assertSetEqual(set(keys()), {"f1", "f2"})
