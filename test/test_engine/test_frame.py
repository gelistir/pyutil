from unittest import TestCase

import pandas as pd

from pyutil.engine.frame import Frame
from test.config import connect

import pandas.util.testing as pdt


class TestFrame(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()
        f = Frame(name="f1").save()
        f.put(frame=pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2,3],[10,20]]))

        g = Frame(name="f2").save()
        g.put(frame=pd.Series(index=["A","B"], data=[1,2]))

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()

    def test_count(self):
        assert Frame.objects.count() == 2

    def test_get_frame(self):
        pdt.assert_frame_equal(Frame.objects(name="f1").first().frame, pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2,3],[10,20]]))

    def test_get_series(self):
        pdt.assert_series_equal(Frame.objects(name="f2").first().frame, pd.Series(index=["A","B"], data=[1,2]))