from pyutil.sql.interfaces.symbols.symbol import Symbol

# this returns a function f(name) = price of Symbol with name==name
def reader(session):
    return lambda name: session.query(Symbol).filter(Symbol.name == name).one().price

def assets(session, names):
    return [session.query(Symbol).filter(Symbol.name == name).one() for name in names]