import abc
import logging

from pyutil.mongo.assets import Assets


class ConfigMaster(object):
    """ Every strategy is described by a configuration object. Each such object inherits from the ConfigMaster class."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, reader, names, logger=None):
        """
        Abstract base class for all strategies

        :param reader: is a function accepting a name as an argument. Returns an Asset...
        :param names: names of the assets used in the strategy
        :param logger: logger
        """

        # the reader in action. Assets takes a list of Asset objects as argument
        self.__reader = reader
        self.__assets = Assets([self.__reader(name) for name in names])

        # this dictionary can be manipulated from the child
        self.configuration = dict()

        # the logger
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    @property
    @abc.abstractmethod
    def group(self)->str:
        """ Group of the configuration """
        return

    @property
    @abc.abstractmethod
    def name(self)-> str:
        """ Name of the Configuration """
        return

    @abc.abstractmethod
    def portfolio(self):
        """ Portfolio described by the Configuration """
        return

    @property
    def assets(self):
        """
        Assets 
        :return:
        """
        return self.__assets

    @property
    def reader(self):
        """ The reader used to load the assets """
        return self.__reader
