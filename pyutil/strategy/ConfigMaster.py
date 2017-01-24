import abc
import logging
from ..mongo.abcArchive import Archive


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, archive, logger=None):
        """
        Abstract base class for all strategies

        :param archive: Archive is the wrapper providing access to historic data
        :param t0: timestamp for starting the strategy
        :param logger: logger
        """
        assert isinstance(archive, Archive), "The archive variable has to be of type Archive. It is {0}".format(type(archive))
        self.__archive = archive
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
        return self.__archive.equity(names=self.symbols)


    def count(self):
        """ Number of assets """
        return len(self.assets)

    def empty(self):
        return self.count() == 0

