import pandas as pd

from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol

# this returns a function f(name) = price of Symbol with name==name
def reader(session):
    return lambda name: session.query(Symbol).filter(Symbol.name == name).one().price

def assets(session, names=None):
    if names is None:
        return session.query(Symbol).all()
    else:
        # todo: use in notation
        return session.query(Symbol).filter(Symbol.name.in_(names)).all()

def portfolio(session, name):
    return session.query(Portfolio).filter(Portfolio.name == name).one().portfolio

def symbolmap(session, names):
    return {asset.name : asset.group.name for asset in assets(session, names)}

def prices(session, names):
    return pd.DataFrame({s.name : s.price for s in assets(session, names)})
