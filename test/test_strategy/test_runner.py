from ming import create_datastore
from pyutil.mongo.reader import _ArchiveReader
from pyutil.mongo.writer import _ArchiveWriter
from test.config import read_frame
from unittest import TestCase


class TestRunner(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = create_datastore("tmp")
        cls.reader = _ArchiveReader(cls.db)
        cls.writer = _ArchiveWriter(cls.db)

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        # write prices into archive
        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

    def test_run(self):
        from pyutil.strategy.Runner import Runner
        module = "test.test_strategy.strat1"

        r = Runner(archive=self.reader, module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)

