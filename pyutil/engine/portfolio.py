# I would love to hide this class better, can't do because Mongo wouldn't like that...
import pandas as pd
from io import BytesIO
from mongoengine import Document, StringField, FileField, DictField
from pyutil.mongo.portfolios import Portfolios
from pyutil.portfolio.portfolio import Portfolio, merge


def __store_portfolio(name, portfolio):
    Porto.objects(name=name).update_one(name=name, upsert=True)
    Porto.objects(name=name).first().put(portfolio=portfolio)

def load_portfolio(name):
    try:
        Porto.objects(name=name).first().portfolio
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

def portfolio_names():
    return set([object.name for object in Porto.objects])

def portfolios():
    return Portfolios({name: load_portfolio(name) for name in portfolio_names()})


class Porto(Document):
    name = StringField(required=True, max_length=200, unique=True)
    price = FileField()
    weight = FileField()
    metadata = DictField(default={})

    def __decodex(self, x):
        return BytesIO(x.read()).read().decode()


    @property
    def portfolio(self):
        p = pd.read_json(self.__decodex(self.price), typ="frame", orient="split")
        w = pd.read_json(self.__decodex(self.weight), typ="weight", orient="split")
        return Portfolio(prices=p, weights=w)

    def put(self, portfolio):
        if self.price:
            self.price.replace(portfolio.prices.to_json(orient="split").encode())

        else:
            self.price.new_file()
            self.price.write(portfolio.prices.to_json(orient="split").encode())
            self.price.close()

        if self.weight:
            self.weight.replace(portfolio.weights.to_json(orient="split").encode())

        else:
            self.weight.new_file()
            self.weight.write(portfolio.weights.to_json(orient="split").encode())
            self.weight.close()

        self.save()
        return self.reload()