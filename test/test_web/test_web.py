import pandas as pd

from unittest import TestCase
from test.config import read_series, read_frame

from pyutil.web.web import series2array, performance2json, frame2html
nav = read_series("nav.csv")


class TestWeb(TestCase):
    def test_series2array(self):
        x = series2array(nav)
        self.assertEqual(x[4][0], 1418770800000)
        self.assertEqual(x[4][1], 1.2875483261929406)

    def test_performance2dict(self):
        x = pd.read_json(performance2json(nav), orient="split", typ="series")
        self.assertEqual(x["Return"], "1.49")

    def test_frame2html(self):
        x = frame2html(read_frame("price.csv"), name="maffay")
        self.assertTrue(x)



