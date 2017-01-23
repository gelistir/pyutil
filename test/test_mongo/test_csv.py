from unittest import TestCase

import pandas as pd
from pyutil.mongo.csvArchive import CsvArchive
from test.config import read_frame
import pandas.util.testing as pdt

class TestCsv(TestCase):
    def setUp(self):
        self.reader = CsvArchive(symbols=read_frame("symbols.csv"), price=read_frame("price.csv", parse_dates=True))

    def test_csv_1(self):
        pdt.assert_frame_equal(self.reader.history(assets=["A","B","C"], name="price"), read_frame("price.csv")[["A","B","C"]])
        pdt.assert_frame_equal(self.reader.history(name="price"), read_frame("price.csv"))

    def test_immutable(self):
        a = self.reader.history(name="price")
        # change the price!
        a["A"]["2014-01-01"] = 2000

        # You can now ask again for the price but since history gives you only a copy and modifications have no impact here
        pdt.assert_frame_equal(self.reader.history(name="price"), read_frame("price.csv"))

    def test_symbols(self):
        pdt.assert_frame_equal(self.reader.reference(), read_frame("symbols.csv"), check_less_precise=True)

    def test_keys(self):
        self.assertListEqual(list(self.reader.keys()), ["price"])

    def test_no_symbols(self):
        r = CsvArchive(price=read_frame("price.csv", parse_dates=True))
        pdt.assert_frame_equal(pd.DataFrame(), r.reference())
        pdt.assert_frame_equal(r.history(name="price"), read_frame("price.csv"))

    def test_immutual(self):
        a = pd.DataFrame(index=["a"], columns=["a"], data=[1.0])
        r = CsvArchive(symbols=a)
        a["a"]["a"] = 3.0
        # Changing this value will have no effect on the Archive
        self.assertEqual(r.reference()["a"]["a"], 1.0)

        # That's the way to change data in an Archive
        r.reference()["a"]["a"] = 2.0
        self.assertEqual(r.reference()["a"]["a"], 2.0)

