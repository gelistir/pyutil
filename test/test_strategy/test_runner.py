from test.config import read_frame
from unittest import TestCase


class Archive(object):
    def __init__(self):
        self.__prices = read_frame("price.csv", parse_dates=True)

    def history(self, items):
        return self.__prices[items]


class TestRunner(TestCase):
    def test_run(self):
        from pyutil.strategy.Runner import Runner
        module = "test.test_strategy.strat1"

        r = Runner(archive=Archive(), module=module)

        self.assertEqual(r.group, "testgroup")
        self.assertEqual(r.name, "test")
        self.assertEqual(r.source[:6], "import")

        s = r.portfolio.summary()
        self.assertAlmostEqual(s[100]["Max Nav"],  1.0771557044604365, places=5)

