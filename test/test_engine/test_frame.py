from unittest import TestCase

import pandas as pd
import numpy as np

from pyutil.engine.frame import store, load, keys, Frame
from pyutil.mongo.connect import connect_mongo

import pandas.util.testing as pdt


class TestFrame(TestCase):
    @classmethod
    def setUpClass(cls):
        connect_mongo('frames', host="testmongo", alias="default")
        frame = pd.DataFrame(index=["A", "B"], columns=["X", "Y"], data=[[2, 3], [10, 20]])
        frame.index.names=["Hans"]

        store(name="f1", frame=frame)

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()

    def test_get_frame(self):
        frame = pd.DataFrame(index=["A", "B"], columns=["X", "Y"], data=[[2, 3], [10, 20]])
        frame.index.names=["Hans"]
        pdt.assert_frame_equal(load(name="f1").frame, frame)

    def test_keys(self):
        self.assertSetEqual(set(keys()), {"f1"})

    def test_multi(self):
        midx = pd.MultiIndex(levels=[['zero', 'one'], ['x', 'y']],
                             labels = [[1, 1, 0, 0], [1, 0, 1, 0]],
                             names=["hans","wurst"])

        df = pd.DataFrame(np.random.randn(4, 2), index=midx, columns=["AA","BB"])
        store(name="maffay", frame=df)
        pdt.assert_frame_equal(load(name="maffay").frame, df)
