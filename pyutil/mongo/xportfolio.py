from pyutil.mongo.mongo import Collection
from pyutil.portfolio.portfolio import Portfolio



def read_portfolio(collection, name):
    prices = Collection.parse(collection.find_one(name=name, kind="PRICES"))
    weights = Collection.parse(collection.find_one(name=name, kind="WEIGHTS"))

    if prices is None and weights is None:
        return None
    else:
        return Portfolio(prices=prices, weights=weights)


def write_portfolio(collection, name, portfolio):
    collection.insert(portfolio.weights, name=name, kind="WEIGHTS")
    collection.insert(portfolio.prices, name=name, kind="PRICES")
