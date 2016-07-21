from pyutil.mongo.archive import writer
from test.config import read_frame
from unittest import TestCase


class TestRunner(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.writer = writer("tmp_ZHJKJFA8", host="mongo", port=27050)

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

    def test_run(self):
        from pyutil.strategy.Runner import Runner

        # specify the module via its name
        module = "test.test_strategy.strat1"

        frame = read_frame("price.csv", parse_dates=True)

        r = Runner(archive=self.writer, module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)

