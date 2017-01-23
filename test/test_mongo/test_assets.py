from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.mongo.assets import from_archive
from pyutil.mongo.csvArchive import CsvArchive
from pyutil.mongo.mongoArchive import MongoArchive
from test.config import read_frame


prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")

class TestAssetsMongo(TestCase):
    @classmethod
    def setUp(self):
        self.archive = MongoArchive()
        self.archive.drop()
        self.archive.symbols.update_all(frame=symbols)
        self.archive.assets.update_all(frame=prices)
        self.assets = from_archive(self.archive, ["A", "B", "C"])


    def test_symbols(self):
        pdt.assert_frame_equal(symbols.ix[["A","B","C"]].sort_index(axis=1), self.assets.symbols.sort_index(axis=1), check_dtype=False)

    def test_history(self):
        self.assertSetEqual(set(self.assets.keys()), {"A", "B", "C"})
        pdt.assert_frame_equal(self.assets.frame("PX_LAST", default=1.0).sort_index(axis=1), prices[["A","B","C"]].sort_index(axis=1))


class TestAssetsCsv(TestCase):
    @classmethod
    def setUp(self):
        self.archive = CsvArchive(symbols=symbols, PX_LAST=prices)
        self.assets = from_archive(self.archive, ["A", "B", "C"])

    def test_symbols(self):
        pdt.assert_frame_equal(symbols.ix[["A","B","C"]].sort_index(axis=1), self.assets.symbols.sort_index(axis=1), check_dtype=False)

    def test_history(self):
        self.assertSetEqual(set(self.assets.keys()), {"A", "B", "C"})
        pdt.assert_frame_equal(self.assets.frame("PX_LAST", default=1.0).sort_index(axis=1), prices[["A","B","C"]].sort_index(axis=1))




