import pandas as pd

from mongoengine import *

from pyutil.engine.aux import flatten, frame2dict
from pyutil.mongo.portfolios import Portfolios
from pyutil.portfolio.portfolio import Portfolio


def portfolios(names=None):
    if names:
        return Portfolios({name: Strat.objects(name=name)[0].portfolio for name in names})
    else:
        return Portfolios({strategy.name: strategy.portfolio for strategy in Strat.objects})


def update_incremental(portfolio, name, group, source, n=5):
    # full write access here...
    s = Strat.objects(name=name).update_one(name=name, group=group, source=source, time=pd.Timestamp("now"), upsert=True)
    s.reload()

    if not s.weights == {}:
        last_valid = s.portfolio.index[-n]
        portfolio = portfolio.truncate(before=last_valid + pd.DateOffset(seconds=1))

    s.update_portfolio(portfolio=portfolio)


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
