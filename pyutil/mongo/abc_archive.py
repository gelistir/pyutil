import abc
import pandas as pd

class Archive(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def history(self, items, name, before=pd.Timestamp("2002-01-01")):
        return

    def history_series(self, item, name="PX_LAST", before=pd.Timestamp("2002-01-01")):
        return self.history(items=[item], name=name, before=before)[item]