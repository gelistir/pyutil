import pandas as pd
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, archive, t0, logger=None):
        super().__init__(archive=archive, assets=["A", "B", "C"], t0=t0, logger=logger)

    def portfolio(self):
        p = self.data()
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / self.count()))

    @property
    def name(self):
        return "test_a"

    @property
    def group(self):
        return "testgroup_a"
