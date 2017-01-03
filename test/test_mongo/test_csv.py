from unittest import TestCase

from pyutil.mongo.csvArchive import CsvArchive
from test.config import read_frame


class TestCsv(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reader = CsvArchive()
        cls.reader.data["price"] = read_frame("price.csv", parse_dates=True)

    def test_csv_1(self):
        a=self.reader.history(["A","B","C"], name="price")
        self.assertAlmostEqual(a["A"]["2014-01-01"], 1200.90, places=6)

    def test_csv_2(self):
        a=self.reader.history(name="price")
        self.assertAlmostEqual(a["A"]["2014-01-01"], 1200.90, places=6)

    #def test_series(self):
    #    a=self.reader.history_series("A", name="price")
    #    self.assertAlmostEqual(a["2014-01-01"], 1200.90, places=6)

