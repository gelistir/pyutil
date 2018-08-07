from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio


class Configuration(ConfigMaster):
    name = "P1"

    def __init__(self, reader=None, **kwargs):
        super().__init__(["A","B"], reader=reader, **kwargs)

    @property
    def portfolio(self):
        return test_portfolio().subportfolio(assets=self.names)