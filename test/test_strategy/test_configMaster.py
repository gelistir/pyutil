from unittest import TestCase

from pyutil.portfolio.portfolio import similar
from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio, read_frame

import pandas.util.testing as pdt


class Strategy(ConfigMaster):
    def __init__(self, names, reader, **kwargs):
        """
        :param names: the names of the assets used in the strategy
        :param reader: a function to access prices for the strategy
        :param kwargs:
        """
        super().__init__(names, reader, **kwargs)

    @property
    def portfolio(self):
        return test_portfolio().subportfolio(assets=self.names)


class TestConfigMaster(TestCase):
    def test_run_strategy(self):
        prices = read_frame("price.csv")

        s = Strategy(names=["A", "B", "C"],             # Assets used by the strategy
                     reader=lambda name: prices[name],  # simple function to read prices
                     Hans=30, Dampf=40)                 # parameter

        # you can still introduce new parameters or change existing ones...
        # The ConigMaster object is a dictionary!
        s["Peter Maffay"] = 20
        s["Hans"] = 35

        pdt.assert_frame_equal(s.history(), prices[["A", "B", "C"]])
        self.assertListEqual(s.names, ["A", "B", "C"])
        self.assertIsNotNone(s.reader)

        parameter = {key: value for key, value in s.items()}
        self.assertDictEqual(parameter, {"Peter Maffay": 20, "Hans": 35, "Dampf": 40})

        pdt.assert_series_equal(s.reader("A"), prices["A"])

        self.assertTrue(similar(s.portfolio, test_portfolio().subportfolio(assets=s.names)))
