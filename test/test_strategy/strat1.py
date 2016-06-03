import pandas as pd

from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, archive, logger=None):
        super().__init__(archive=archive, logger=logger)
        self.configuration["assets"] = ["A", "B", "C"]
        self.configuration["start"] = pd.Timestamp("2002-01-01")

    def portfolio(self):
        a = self.configuration["assets"]
        p = self.archive.history(items=a, before=self.configuration["start"])
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / len(a)))

    @property
    def name(self):
        return "test"

    @property
    def group(self):
        return "testgroup"
