import pandas as pd

from pyutil.engine.frame import load, store, Frame
from pyutil.mongo.portfolios import Portfolios
from pyutil.portfolio.portfolio import Portfolio, merge


def store_portfolio(name, portfolio):
    store(name + "_price", portfolio.prices)
    store(name + "_weight", portfolio.weights)


def load_portfolio(name):
    try:
        prices = load(name + "_price").frame
        weights = load(name + "_weight").frame
        return Portfolio(prices=prices, weights=weights)
    except AttributeError:
        return None


def update_portfolio(name, portfolio):
    p_old = load_portfolio(name)
    if p_old:
        start = portfolio.index[0]
        p_old = p_old.truncate(after=start - pd.offsets.Second(n=1))
        store_portfolio(name, merge([p_old, portfolio], axis=0))
    else:
        store_portfolio(name, portfolio)


def portfolio_names():
    x = [object.name for object in Frame.objects]
    x = [a.replace("_price", "") for a in x]
    x = [a.replace("_weight", "") for a in x]
    return set(x)


def portfolios():
    return Portfolios({name: load_portfolio(name) for name in portfolio_names()})


if __name__ == '__main__':
    from pyutil.mongo.connect import connect_mongo
    # connect to a mongo database for storing the frames
    con_mongo = connect_mongo(db="factsheet", host="quantsrv")

    print(portfolio_names())
    print(portfolios())
    p1 = load_portfolio("MDT")
    print(p1)

    update_portfolio("wurst", p1)
    xxx = load_portfolio("wurst")
