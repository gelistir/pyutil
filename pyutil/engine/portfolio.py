import warnings

import collections
import pandas as pd

from mongoengine import *

from pyutil.portfolio.portfolio import Portfolio


def portfolio_names():
    return set([portfolio.name for portfolio in Strat.objects])


def portfolio_builder(name):
    sym = Strat.objects(name=name)[0]
    p = sym.portfolio
    p.meta["group"] = sym.group
    p.meta["comment"] = sym.source
    p.meta["time"] = sym.time
    return p
    #return Portfolio(name=sym.name, prices=sym.prices, weights=sym.weights, group=sym.group, comment=sym.source, time=sym.time)


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
        return Portfolio(prices=f(self.prices), weights=f(self.weights))

    def update_portfolio(self, portfolio):
        if not portfolio.empty:
            w = Strat.__mongo(portfolio.weights)
            p = Strat.__mongo(portfolio.prices)
            if self.weights == {} and self.prices == {}:
                self.update(weights=w, prices=p)
            else:
                self._get_collection().update({"name": self.name},
                                              {"$set": Strat.__flatten({**{"weights": w}, **{"prices": p}})},
                                              upsert=True)

        return self.portfolio