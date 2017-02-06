import abc
import logging

from pyutil.mongo.assets import Assets


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, assets, logger=None):
        """
        Abstract base class for all strategies

        :param assets: Archive is the wrapper providing access to historic data
        :param t0: timestamp for starting the strategy
        :param logger: logger
        """
        assert isinstance(assets, Assets), "The assets variable has to be of type Assets. It is {0}".format(type(assets))
        self.__assets = assets
        self.configuration = dict()
        self.logger = logger or logging.getLogger(__name__)
        self.symbols = []


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
        # this is expensive. Avoid calling it too often...
        return self.__assets.sub(assets=self.symbols)


    def count(self):
        """ Number of assets """
        return len(self.assets)

    def empty(self):
        return self.count() == 0

