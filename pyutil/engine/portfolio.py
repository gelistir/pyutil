import collections
import pandas as pd

from mongoengine import *

from pyutil.mongo.portfolios import Portfolios
from pyutil.portfolio.portfolio import Portfolio


def portfolios(names=None):
    p = Portfolios()
    if names:
        for name in names:
            p[name] = Strat.objects(name=name)[0].portfolio
    else:
        for strategy in Strat.objects:
            p[strategy.name] = strategy.portfolio
    return p

class Strat(Document):
    name = StringField(required=True, max_length=200, unique=True)
    group = StringField(max_length=200)
    weights = DictField()
    prices = DictField()
    time = DateTimeField()
    source = StringField()

    @staticmethod
    def __flatten(d, parent_key=None, sep='.'):
        """ flatten dictonaries of dictionaries (of dictionaries of dict... you get it)"""
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(Strat.__flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def __mongo(x):
        y = x.copy()
        y.index = [a.strftime("%Y%m%d") for a in y.index]
        return {k: v.dropna().to_dict() for k, v in y.items()}

    @property
    def portfolio(self):
        def f(x):
            x = pd.DataFrame(x)
            x.index = [pd.Timestamp(a) for a in x.index]
            return x

        x = Portfolio(prices=f(self.prices), weights=f(self.weights))
        x.meta["group"] = self.group
        x.meta["comment"] = self.source
        x.meta["time"] = self.time
        return x

    def update_portfolio(self, portfolio):
        if not portfolio.empty:
            w = Strat.__mongo(portfolio.weights)
            p = Strat.__mongo(portfolio.prices)
            if self.weights == {} and self.prices == {}:
                # fresh data...
                self.update(weights=w, prices=p)
            else:
                self._get_collection().update({"name": self.name},
                                              {"$set": Strat.__flatten({**{"weights": w}, **{"prices": p}})},
                                              upsert=True)

        return Strat.objects(name=self.name)[0]

