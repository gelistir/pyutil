import importlib
import os
from pyutil.log import get_logger


class Runner(object):
    def __init__(self, module, archive, logger=None):
        self.__logger = logger or get_logger("LOBNEK")

        # see: http://stackoverflow.com/questions/37209181/converting-the-py-script-into-a-string/37209507#37209507
        module = importlib.import_module(module)
        new_module_filename = os.path.realpath(module.__file__)

        if new_module_filename.endswith(".pyc"):
            new_module_filename = new_module_filename[:-1]

        with open(new_module_filename) as ff:
            self.__source = ff.read()

        self.__config = module.Configuration(archive=archive, logger=self.__logger)

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
