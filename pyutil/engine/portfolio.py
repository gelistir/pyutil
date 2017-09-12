# I would love to hide this class better, can't do because Mongo wouldn't like that...
from io import BytesIO

import pandas as pd
from mongoengine import Document, StringField, DictField, BinaryField

from pyutil.mongo.portfolios import Portfolios
from pyutil.portfolio.portfolio import Portfolio, merge


def __store_portfolio(name, portfolio):
    PortfolioMongo.objects(name=name).update_one(name=name, upsert=True)
    PortfolioMongo.objects(name=name).first().put(portfolio=portfolio)


def load_portfolio(name):
    try:
        return PortfolioMongo.objects(name=name).first().portfolio
    except AttributeError:
        return None


def upsert_portfolio(name, portfolio):
    p_old = load_portfolio(name)
    if p_old:
        start = portfolio.index[0]
        p_old = p_old.truncate(after=start - pd.offsets.Second(n=1))
        __store_portfolio(name, merge([p_old, portfolio], axis=0))
    else:
        __store_portfolio(name, portfolio)

    return load_portfolio(name)


def save_portfolio(name, portfolio):
    __store_portfolio(name, portfolio)


#def portfolio_names():
#    return set([object.name for object in Porto.objects])


def portfolios():
    return Portfolios({object.name : object.portfolio for object in PortfolioMongo.objects})


# class Porto(Document):
#     name = StringField(required=True, max_length=200, unique=True)
#     price = FileField(collection_name="portfolio_files")
#     weight = FileField(collection_name="portfolio_files")
#     metadata = DictField(default={})
#
#     @staticmethod
#     def __decodex(x):
#         return BytesIO(x.read()).read().decode()
#
#     @property
#     def portfolio(self):
#         p = pd.read_json(self.__decodex(self.price), typ="frame", orient="split")
#         w = pd.read_json(self.__decodex(self.weight), typ="frame", orient="split")
#         return Portfolio(prices=p, weights=w)
#
#     # def put(self, portfolio):
#     #     g = lambda x: x.to_json(orient="split").encode()
#     #
#     #     if self.price:
#     #         self.price.replace(g(portfolio.prices))
#     #
#     #     else:
#     #         self.price.new_file()
#     #         self.price.write(g(portfolio.prices))
#     #         self.price.close()
#     #
#     #     if self.weight:
#     #         self.weight.replace(g(portfolio.weights))
#     #
#     #     else:
#     #         self.weight.new_file()
#     #         self.weight.write(g(portfolio.weights))
#     #         self.weight.close()
#     #
#     #     self.save()
#     #     return self.reload()


class PortfolioMongo(Document):
    name = StringField(required=True, max_length=200, unique=True)
    price = BinaryField()
    weight = BinaryField()
    metadata = DictField(default={})

    @staticmethod
    def __decodex(x):
        return BytesIO(x).read().decode()

    @property
    def portfolio(self):
        p = pd.read_json(self.__decodex(self.price), typ="frame", orient="split")
        w = pd.read_json(self.__decodex(self.weight), typ="frame", orient="split")
        return Portfolio(prices=p, weights=w)

    def put(self, portfolio):
        g = lambda x: x.to_json(orient="split").encode()

        self.price = g(portfolio.prices)
        self.weight = g(portfolio.weights)

        self.save()
        return self.reload()
