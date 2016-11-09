import pandas as pd
from unittest import TestCase

from pyutil.mongo.csv import CsvArchive
from test.config import read_frame


class TestCsv(TestCase):
    def test_csv(self):
        reader = CsvArchive()
        reader.put("price", frame=read_frame("price.csv", parse_dates=True))
        a=reader.history(["A","B","C"], name="price", before=pd.Timestamp("2014-01-01"))
        self.assertAlmostEqual(a["A"][pd.Timestamp("2014-01-01")], 1200.90, places=6)

    def test_series(self):
        reader = CsvArchive()
        reader.put("price", frame=read_frame("price.csv", parse_dates=True))
        a=reader.history_series("A", name="price", before=pd.Timestamp("2014-01-01"))
        self.assertAlmostEqual(a[pd.Timestamp("2014-01-01")], 1200.90, places=6)

