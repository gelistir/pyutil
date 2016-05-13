from pymongo import MongoClient
from pymongo.database import Database
from pyutil.mongo.archive import writer, reader
from test.config import read_frame, test_portfolio
from unittest import TestCase


class TestRunner(TestCase):
    @classmethod
    def setUpClass(cls):

        cls.client = MongoClient("quantsrv", port=27017)
        cls.db = Database(cls.client, "tmp")

        cls.writer = writer("tmp")
        cls.reader = reader("tmp")

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

        frame = read_frame("symbols.csv")
        cls.writer.update_symbols(frame)

        p = test_portfolio()
        cls.writer.update_portfolio("test", p, group="test")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.db)

    def test_run(self):
        from pyutil.strategy.Runner import Runner
        module = "test.test_strategy.strat1"

        r = Runner(archive=self.reader, module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)

