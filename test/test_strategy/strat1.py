import pandas as pd

from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, archive, t0, logger=None):
        super().__init__(archive=archive, t0=t0, logger=logger)
        self.configuration["prices"] = self.prices(["A", "B", "C"])

    def portfolio(self):
        p = self.configuration["prices"]
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / len(p.keys())))


    @property
    def name(self):
        return "test"

    @property
    def group(self):
        return "testgroup"
