import abc
import pandas as pd
import logging
from typing import List
from ..mongo.abc_archive import Archive


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, archive: Archive, t0=pd.Timestamp("2002-01-01"), logger=None):
        """
        Abstract base class for all strategies

        :param archive: Archive is the wrapper providing access to historic data
        :param t0: timestamp for starting the strategy
        :param logger: logger
        """
        assert isinstance(archive, Archive), "The archive variable has to be of type Archive. It is {0}".format(type(archive))
        self.configuration = dict()
        self.logger = logger or logging.getLogger(__name__)
        self.archive = archive
        self.t0 = t0

    @abc.abstractproperty
    def group(self)->str:
        return

    @abc.abstractproperty
    def name(self)->str:
        return

    @abc.abstractmethod
    def portfolio(self):
        return

    def prices(self, assets: List[str]) -> pd.DataFrame:
        return self.archive.history(items=assets).truncate(before=self.t0)