# from unittest import TestCase
#
# from pyutil.sql.base import Base
# from pyutil.sql.interfaces.symbols.portfolio import Portfolio
# from pyutil.sql.interfaces.symbols.strategy import Strategy
# from pyutil.sql.interfaces.symbols.symbol import Symbol
# from pyutil.sql.session import postgresql_db_test
# from pyutil.strategy.loop import strategy_loop
# from test.config import test_portfolio, resource
#
#
# class TestConfigMaster(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         # create a session
#         cls.session = postgresql_db_test(base=Base, echo=False)
#
#         s = [Symbol(name=s) for s in test_portfolio().assets]
#         cls.session.add_all(s)
#         cls.session.commit()
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.session.close()
#
#     def test_loop(self):
#         # load the test_portfolio
#         p = test_portfolio()
#         # locate the folder containing the strategies
#         folder = resource("strat")
#
#         symbols = {s.name: s for s in self.session.query(Symbol)}
#
#         # loop over those strategies and update the underlying data
#         for portfolio, strategy in strategy_loop(session=self.session, folder=folder, reader=lambda name, field: p.prices[name]):
#             print(portfolio)
#             print(strategy.name)
#             strategy.upsert(portfolio=portfolio, symbols=symbols)
#
#         for p in self.session.query(Strategy):
#             print(p.name)
#             print(p._portfolio.name)
#
#         for p in self.session.query(Portfolio):
#             print(p.name)
#
