import abc

class Archive(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def history(self, items=None, name="PX_LAST"):
        return

    def history_series(self, item, name="PX_LAST"):
        return self.history(items=[item], name=name)[item]

