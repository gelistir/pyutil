# from unittest import TestCase
#
# from pyutil.sql.models import _Base, Symbol, PortfolioSQL, SymbolType
# from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance
# from pyutil.sql.session import session_test, session_scope
# from test.config import test_portfolio
#
#
# class TestReport(TestCase):
#
#     def test_strategy_2(self):
#         with session_scope(session=session_test(meta=_Base.metadata)) as session:
#             for symbol in ["A","B","C","D"]:
#                 s = Symbol(bloomberg_symbol=symbol, group=SymbolType.equities)
#                 session.add(s)
#
#             for symbol in ["E", "F", "G"]:
#                 s = Symbol(bloomberg_symbol=symbol, group=SymbolType.equities)
#                 session.add(s)
#
#             #session.add_all([g1, g2])
#             p = PortfolioSQL(name="test")
#             p.upsert(portfolio=test_portfolio())
#
#             session.add(p)
#
#             print(mtd(session))
#             print(ytd(session))
#             print(mtd(session, names=["test"]))
#             print(ytd(session, names=["test"]))
#
#             print(sector(session))
#             print(recent(session))
#             print(period_returns(session))
#             print(performance(session))
