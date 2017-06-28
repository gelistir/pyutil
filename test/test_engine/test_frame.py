from unittest import TestCase

import pandas as pd

from pyutil.engine.frame import store, load, Frame
from test.config import connect

import pandas.util.testing as pdt


class TestFrame(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()
        store(name="f1", object=pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2,3],[10,20]]))
        store(name="f2", object=pd.Series(index=["A","B"], data=[1,2]))

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()

    def test_count(self):
        assert Frame.objects.count() == 2

    def test_get_frame(self):
        pdt.assert_frame_equal(load(name="f1").frame, pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2,3],[10,20]]))

    def test_get_series(self):
        pdt.assert_series_equal(load(name="f2").frame, pd.Series(index=["A","B"], data=[1,2]))