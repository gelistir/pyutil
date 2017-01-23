import abc


class Archive(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def history(self, assets=None, name="PX_LAST"):
        """
        Historic data for some assets
        :param assets: List of assets, if not specified will return data for all assets with a "name" time series
        :param name: name of the time series
        :return: DataFrame of historic data
        """
        return

    @abc.abstractclassmethod
    def symbols(self):
        return
