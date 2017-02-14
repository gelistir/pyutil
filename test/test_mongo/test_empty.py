from unittest import TestCase
from pyutil.mongo.mongoArchive import MongoArchive


# An empty database requires special care, we do that here...
class TestMongoArchive(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        cls.archive.time_series.drop()
        cls.archive.portfolios.drop()
        cls.archive.symbols.drop()

    def test_history_empty(self):
        x = self.archive.history("PX_LAST")
        self.assertTrue(x.empty)
        self.assertFalse("Peter" in x.keys())

    def test_symbols_empty(self):
        x = self.archive.reference
        self.assertTrue(x.empty)

    def test_strategies(self):
        x = self.archive.strategies
        self.assertIsNone(x)

    def test_reader(self):
        with self.assertRaises(KeyError):
            self.archive.reader("Peter Maffay")



