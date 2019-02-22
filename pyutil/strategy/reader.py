from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol

# this returns a function f(name) = price of Symbol with name==name
def reader(session):
    return lambda name: session.query(Symbol).filter(Symbol.name == name).one().price

def assets(session, names):
    return [session.query(Symbol).filter(Symbol.name == name).one() for name in names]

def portfolio(session, name):
    return session.query(Portfolio).filter(Portfolio.name == name).one()
