import uuid
from unittest import TestCase
from pyutil.engine.symbol import Symbol, asset_builder

from mongoengine import connect

# An empty database requires special care, we do that here...
class TestMongoArchive(TestCase):
    @classmethod
    def setUpClass(cls):
        connect(db="testEmpty", alias="default")
        Symbol.drop_collection()

        #cls.archive = MongoArchive()
        #cls.archive.time_series.drop()
        #cls.archive.portfolios.drop()
        #cls.archive.symbols.drop()

    def test_history_empty(self):
        x = Symbol.history()
        self.assertTrue(x.empty)
        self.assertFalse("Peter" in x.keys())

    #def test_symbols_empty(self):
    #    x = self.archive.reference
    #    self.assertTrue(x.empty)

    #def test_strategies(self):
    #    x = .strategies
    #    self.assertIsNone(x)

    def test_reader(self):
        with self.assertRaises(IndexError):
            asset_builder(name="Peter Maffay")

    def test_loop(self):
        assert Symbol.objects.count() == 0, "It is {0}".format(Symbol.objects.count())




