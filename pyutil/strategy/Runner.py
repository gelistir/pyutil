import importlib
import logging
import inspect


class Runner(object):
    def __init__(self, module, archive, t0, logger=None):
        # see: http://stackoverflow.com/questions/37209181/converting-the-py-script-into-a-string/37209507#37209507
        module = importlib.import_module(module)
        self.__source = inspect.getsource(object=module)
        self.__config = module.Configuration(archive=archive, t0=t0, logger=logger)

        # compute the portfolio
        self.__portfolio = self.__config.portfolio()

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
