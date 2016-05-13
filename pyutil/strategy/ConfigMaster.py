import abc
import pandas as pd
from pyutil.log import get_logger


class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, archive, logger=None):
        self.configuration = dict()
        self.logger = logger or get_logger("LOBNEK")
        self.archive = archive

    @abc.abstractproperty
    def group(self):
        return

    @abc.abstractproperty
    def name(self):
        return

    @abc.abstractmethod
    def method(self):
        return

    def prices(self, assets, before=pd.Timestamp("2002-01-01")):
        return self.archive.history(items=assets).truncate(before=before)