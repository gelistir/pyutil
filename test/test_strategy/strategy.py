# from pyutil.strategy.config import ConfigMaster
#
#
# class Strategy(ConfigMaster):
#     def __init__(self, names, reader, **kwargs):
#         """
#         :param names: the names of the assets used in the strategy
#         :param reader: a function to access prices for the strategy
#         :param kwargs:
#         """
#         super().__init__(names, reader, **kwargs)
#
#     @property
#     def portfolio(self):
#         return portfolio.subportfolio(assets=self.names)
