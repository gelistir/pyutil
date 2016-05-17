import pandas as pd

from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, archive, logger=None):
        super().__init__(archive=archive, logger=logger)
        self.configuration["assets"] = ["A", "B", "C"]
        self.configuration["prices"] = self.prices(assets=self.configuration["assets"])

    @property
    def name(self):
        return "test"

    def method(self):
        p = self.configuration["prices"]
        return Portfolio(self.configuration["prices"],
                     weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / 3.0))

    @property
    def group(self):
        return "testgroup"
