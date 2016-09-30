import abc
import pandas as pd
import logging

class ConfigMaster(object):
    __metaclass__ = abc.ABCMeta

    def __repr__(self, *args, **kwargs):
        return 'Name: {0}, Group: {1}'.format(self.name, self.group)

    def __init__(self, archive, t0=pd.Timestamp("2002-01-01"), logger=None):
        self.configuration = dict()
        self.logger = logger or logging.getLogger(__name__)
        self.archive = archive
        self.t0 = t0

    @abc.abstractproperty
    def group(self):
        return

    @abc.abstractproperty
    def name(self):
        return

    @abc.abstractmethod
    def portfolio(self):
        return

    def prices(self, assets):
        return self.archive.history(items=assets).truncate(before=self.t0)