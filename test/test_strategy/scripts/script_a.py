import pandas as pd

from pyutil.mongo.assets import Assets
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, reader, logger=None):
        super().__init__(reader=reader, logger=logger)

    def portfolio(self):
        # extract the assets (using the reader)
        p = self.assets(names=["A", "B", "C"]).frame()
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / 3.0))

    @property
    def name(self):
        return "test_a"

    @property
    def group(self):
        return "testgroup_a"
