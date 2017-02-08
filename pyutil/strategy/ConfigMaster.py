import abc
import logging

import pandas as pd

from pyutil.mongo.assets import Assets


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, reader, logger=None):
        """
        Abstract base class for all strategies

        :param reader: is a function accepting a name as an argument. Returns an Asset...
        :param t0: timestamp for starting the strategy
        :param logger: logger
        """
        self.__reader = reader
        self.configuration = dict()
        self.logger = logger or logging.getLogger(__name__)


    @abc.abstractproperty
    def group(self)->str:
        return

    @abc.abstractproperty
    def name(self)-> str:
        return

    @abc.abstractmethod
    def portfolio(self):
        return

    def asset(self, name):
        return self.__reader(name)

    def frame(self, names, key="PX_LAST", before=pd.Timestamp("2002-01-01")):
        return self.assets(names=names).frame(key=key).dropna(axis=0, how="all").truncate(before=before)

    def assets(self, names):
        """
        Using the reader construct an Assets object
        :param names:
        :return:
        """
        return Assets([self.asset(name) for name in names])
