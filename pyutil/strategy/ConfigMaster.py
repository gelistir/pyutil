import abc
import logging

from pyutil.mongo.assets import Assets


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, reader, names, logger=None):
        """
        Abstract base class for all strategies

        :param reader: is a function accepting a name as an argument. Returns an Asset...
        :param t0: timestamp for starting the strategy
        :param logger: logger
        """
        self.__assets = Assets([reader(name) for name in names])

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

    @property
    def assets(self):
        """
        Using the reader construct an Assets object
        :param names:
        :return:
        """
        return self.__assets