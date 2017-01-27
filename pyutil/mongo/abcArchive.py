import abc

from pyutil.mongo.assets import Assets


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
    def reference(self):
        return


    @abc.abstractmethod
    def asset(self, name):
        """
        Construct an asset based on a name
        :param name:
        :return: An asset (see
        """
        return

    def equity(self, names):
        """
        Construct assets based on a list of names
        """
        assert not len(names) == 0, "Please specify a list of assets."
        return Assets([self.asset(name) for name in names])
