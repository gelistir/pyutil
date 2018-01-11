from test.config import test_portfolio


class Configuration(object):
    def __init__(self, reader):
        self.__reader = reader

    @property
    def portfolio(self):
        return test_portfolio()

    @property
    def assets(self):
        return test_portfolio().assets
