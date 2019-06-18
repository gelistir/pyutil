# import os
# from pyutil.sql.interfaces.symbols.symbol import Symbol
# from pyutil.sql.interfaces.symbols.portfolio import Portfolio
# from pyutil.sql.session import session_factory
# from pyutil.strategy.reader import reader
#
# # create a session, this will not loose scope within the Notebook
# session = session_factory(os.environ["reader"], echo=False)
#
# # create the read_price function
# read_price = reader(session=session)
#
# # create the symbolmap
# symbolmap = {asset.name: asset.group.name for asset in session.query(Symbol).all()}
#
#
# # read portfolio from database
# def read_portfolio(name):
#     p = session.query(Portfolio).filter(Portfolio.name == name).one().portfolio
#     p.symbolmap = symbolmap
#     return p
#
#
# def read_assets(names):
#     return session.query(Symbol).filter(Symbol.name.in_(names)).all()
