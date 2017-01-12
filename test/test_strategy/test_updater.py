from unittest import TestCase

from pyutil.mongo.mongoArchive import MongoArchive
from pyutil.strategy.Loop import Result
from pyutil.strategy.Updater import update_portfolio
from test.config import test_portfolio

class TestUpdater(TestCase):
    def test_update(self):
        archive = MongoArchive()
        portfolio = test_portfolio()

        r = Result(name="test", group="test", source="test", portfolio=portfolio)
        with self.assertWarns(Warning):
            # unknown portfolio
            update_portfolio(archive=archive, result=r, n=5)

        # now we update the portfolio, no warning, no error, using only the last 5 points
        update_portfolio(archive=archive, result=r, n=5)
