import importlib
from pyutil.log import get_logger


class Runner(object):
    def __init__(self, module, archive, logger=None):
        self.__logger = logger or get_logger("LOBNEK")

        module = importlib.import_module(module)
        with open(module.__file__) as ff:
            self.__source = ff.read()

        self.__logger.debug(self.__source)

        self.__config = module.Configuration(archive=archive, logger=self.__logger)

        # compute the portfolio
        self.__portfolio = self.__config.method()

    @property
    def portfolio(self):
        return self.__portfolio

    @property
    def name(self):
        return self.__config.name

    @property
    def group(self):
        return self.__config.group

    @property
    def source(self):
        return self.__source
