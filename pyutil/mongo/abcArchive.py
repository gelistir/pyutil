import abc


class Archive(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def history(self, assets=None, name="PX_LAST"):
        """
        Access historic data for some assets
        :param assets: List of assets
        :param name: Name of the time series
        :return:
        """
        return
