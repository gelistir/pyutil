from unittest import TestCase

import pandas as pd

from pyutil.mongo.mongoArchive import MongoArchive
from pyutil.strategy.Updater import update_portfolio
from test.config import test_portfolio

class TestUpdater(TestCase):
    def test_update(self):
        archive = MongoArchive()
        portfolio = test_portfolio()
        portfolio.meta["time"] = pd.Timestamp("now")
        portfolio.meta["group"] = "g"
        portfolio.meta["comment"] = ""

        #r = Result(name="test", portfolio=portfolio)
        with self.assertWarns(Warning):
            # unknown portfolio
            update_portfolio(archive=archive, name="test", portfolio=portfolio, n=5)

        # now we update the portfolio, no warning, no error, using only the last 5 points
        update_portfolio(archive=archive, name="test", portfolio=portfolio, n=5)
