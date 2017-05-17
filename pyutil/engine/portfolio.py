import pandas as pd

from mongoengine import *

from pyutil.engine.aux import flatten, dict2frame, frame2dict
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


def from_portfolio(portfolio, name, group, time=pd.Timestamp("now"), source=""):
    return Strat(name=name, weights=frame2dict(portfolio.weights), prices=frame2dict(portfolio.prices), group=group, time=time, source=source)


def update_incremental(portfolio, name, group, source, n=5):
    # full write access here...

    s = Strat.objects(name=name)
    if len(s) == 0:
        p = from_portfolio(portfolio=portfolio, name=name, group=group, source=source)
        p.save()
    else:
        object = s[0]
        object.update(time=pd.Timestamp("now"), source=source, group=group)

        # truncate the portfolio...
        last_valid = object.portfolio.index[-n]
        portfolio = portfolio.truncate(before=last_valid + pd.DateOffset(seconds=1))
        object.update_portfolio(portfolio=portfolio)


class Strat(Document):
    name = StringField(required=True, max_length=200, unique=True)
    group = StringField(max_length=200)
    weights = DictField()
    prices = DictField()
    time = DateTimeField()
    source = StringField()

    @property
    def portfolio(self):
        def f(x):
            x = pd.DataFrame(x)
            x.index = [pd.Timestamp(a) for a in x.index]
            return x

        x = Portfolio(prices=f(self.prices), weights=f(self.weights))
        #x.meta["group"] = self.group
        #x.meta["comment"] = self.source
        #x.meta["time"] = self.time
        return x

    def update_portfolio(self, portfolio):
        if not portfolio.empty:
            w = frame2dict(portfolio.weights)
            p = frame2dict(portfolio.prices)
            if self.weights == {} and self.prices == {}:
                # fresh data...
                self.update(weights=w, prices=p)
            else:
                self._get_collection().update({"name": self.name},
                                              {"$set": flatten({**{"weights": w}, **{"prices": p}})},
                                              upsert=True)

        return Strat.objects(name=self.name)[0]

