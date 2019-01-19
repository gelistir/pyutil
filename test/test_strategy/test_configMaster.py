import pandas.util.testing as pdt

from pyutil.portfolio.portfolio import similar
from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio, read


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


class TestConfigMaster(object):
    def test_run_strategy(self):
        prices = read("price.csv", parse_dates=True)

        s = Strategy(names=["A", "B", "C"],             # Assets used by the strategy
                     reader=lambda name: prices[name],  # simple function to read prices
                     Hans=30, Dampf=40)                 # parameter

        # you can still introduce new parameters or change existing ones...
        # The ConigMaster object is a dictionary!
        s["Peter Maffay"] = 20
        s["Hans"] = 35

        pdt.assert_frame_equal(s.history(), prices[["A", "B", "C"]])
        assert s.names == ["A", "B", "C"]
        assert s.reader

        #self.assertIsNotNone(s.reader)

        parameter = {key: value for key, value in s.items()}
        assert parameter == {"Peter Maffay": 20, "Hans": 35, "Dampf": 40}

        pdt.assert_series_equal(s.reader("A"), prices["A"])

        assert similar(s.portfolio, test_portfolio().subportfolio(assets=s.names))
