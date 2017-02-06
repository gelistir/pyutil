import pandas as pd
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, assets, logger=None):
        super().__init__(assets=assets, logger=logger)
        self.symbols = ["A", "B", "C", "D"]

    def portfolio(self):
        p = self.assets.frame()
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / self.count()))

    @property
    def name(self):
        return "test_b"

    @property
    def group(self):
        return "testgroup_b"
