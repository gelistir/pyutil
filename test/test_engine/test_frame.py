from unittest import TestCase

import pandas as pd

from pyutil.engine.frame import Frame
from test.config import connect

import pandas.util.testing as pdt

class TestFrame(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()
        Frame(name="f1", data=pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2.0,3.0],[10.0,20.0]])).save()

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()

    def test_count(self):
        assert Frame.objects.count() == 1

    def test_get(self):
        pdt.assert_frame_equal(Frame.objects(name="f1").first().frame, pd.DataFrame(index=["A","B"], columns=["X","Y"], data=[[2.0,3.0],[10.0,20.0]]))

