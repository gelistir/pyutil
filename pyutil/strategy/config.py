import abc
import logging
import pandas as pd
from pyutil.sql.interfaces.symbols.symbol import Symbol


class ConfigMaster(dict):
    """ Every strategy is described by a configuration object. Each such object inherits from the ConfigMaster class."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, names, reader=None, logger=None, **kwargs):
        """
        Abstract base class for all strategies

        :param reader: is a function accepting a name as an argument. Returns an Asset...
        :param names: names of the assets used in the strategy
        :param logger: logger
        """
        super().__init__(**kwargs)

        # the logger
        self.logger = logger or logging.getLogger(__name__)
        self.__reader = reader or Symbol.symbol
        self.__names = names

    @property
    @abc.abstractmethod
    def portfolio(self):
        """ Portfolio described by the Configuration """

    @property
    def names(self):
        return self.__names

    @property
    def reader(self):
        return self.__reader

    def history(self, field="PX_LAST", t0=pd.Timestamp("2002-01-01")):
        h = pd.DataFrame({name: self.__reader(name=name, field=field) for name in self.__names})
        return h.truncate(before=t0).dropna(axis=0, how="all")
