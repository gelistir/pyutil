from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio


class Configuration(ConfigMaster):
    name = "P2"

    def __init__(self, reader=None, **kwargs):
        super().__init__(["A","B","C"], reader=reader, **kwargs)

    @property
    def portfolio(self):
        return test_portfolio().subportfolio(assets=self.names)