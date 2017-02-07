import pandas as pd
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster


class Configuration(ConfigMaster):
    def __init__(self, reader, logger=None):
        super().__init__(reader=reader, logger=logger)
        #self.symbols = ["A", "B", "C", "D"]

    def portfolio(self):
        #assets = self.assets(names=["A", "B", "C", "D"])#.frame()
        # print(assets)
        # introduce a new DataFrame
        # p = assets.frame()
        #assets["weight"] = pd.DataFrame(index=p.index, columns=p.keys(), data=0.25)
        #print(assets)
        #assert False

        p = self.frame(names=["A", "B", "C", "D"])
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / 4.0))

    @property
    def name(self):
        return "test_b"

    @property
    def group(self):
        return "testgroup_b"
