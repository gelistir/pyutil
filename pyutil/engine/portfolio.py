# import pandas as pd
#
# from mongoengine import *
#
# from pyutil.engine.aux import flatten, frame2dict
# from pyutil.mongo.portfolios import Portfolios
# from pyutil.portfolio.portfolio import Portfolio
#
#
# def portfolios(names=None):
#     if names:
#         return Portfolios({name: portfolio(name).portfolio for name in names})
#     else:
#         return Portfolios({strategy.name: strategy.portfolio for strategy in Strat.objects})
#
#
# def portfolio(name, upsert=False):
#     if upsert:
#         Strat.objects(name=name).update_one(name=name, upsert=True)
#     s = Strat.objects(name=name).first()
#     assert s, "The Portfolio {name} is unknown".format(name=name)
#     return s
#
#
# class Strat(Document):
#     name = StringField(required=True, max_length=200, unique=True)
#     group = StringField(max_length=200)
#     weights = DictField()
#     prices = DictField()
#     time = DateTimeField(default=pd.Timestamp("now"))
#     source = StringField(default="")
#
#     @property
#     def portfolio(self):
#         def f(x):
#             x = pd.DataFrame(x)
#             x.index = [pd.Timestamp(a) for a in x.index]
#             return x
#
#         return Portfolio(prices=f(self.prices), weights=f(self.weights))
#
#     @property
#     def empty(self):
#         return self.weights == {} and self.prices == {}
#
#     def update_portfolio(self, portfolio):
#         if not portfolio.empty:
#             w = frame2dict(portfolio.weights)
#             p = frame2dict(portfolio.prices)
#             if self.empty:
#                 # fresh data...
#                 self.update(weights=w, prices=p)
#             else:
#                 self._get_collection().update({"name": self.name},
#                                               {"$set": flatten({**{"weights": w}, **{"prices": p}})})
#
#         return self.reload()
