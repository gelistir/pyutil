from pyutil.mongo.reader import CsvArchive
from test.config import read_frame
from unittest import TestCase


class TestRunner(TestCase):
    def test_run(self):
        from pyutil.strategy.Runner import Runner

        # specify the module via its name
        module = "test.test_strategy.strat1"

        frame = read_frame("price.csv", parse_dates=True)

        r = Runner(archive=CsvArchive(frame), module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)

