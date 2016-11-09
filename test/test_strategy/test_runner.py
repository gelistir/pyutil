import pandas as pd

from pyutil.mongo.archive import writer
from test.config import read_frame
from unittest import TestCase

from pyutil.strategy.Runner import run

class TestRunner(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.writer = writer("tmp_ZHJKJFA8", host="mongo")
        cls.writer.update_assets(read_frame("price.csv", parse_dates=True))


    def test_run(self):
        # specify the module via its name
        module = "test.test_strategy.strat1"

        r = run(archive=self.writer, t0=pd.Timestamp("2002-01-01"), module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)
