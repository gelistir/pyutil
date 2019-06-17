from pyutil.mongo.mongo import Collection
from pyutil.portfolio.portfolio import Portfolio



def portfolio(collection, name):
    prices = Collection.parse(collection.find_one(name=name, kind="PRICES"))
    weights = Collection.parse(collection.find_one(name=name, kind="WEIGHTS"))
    return Portfolio(prices=prices, weights=weights)