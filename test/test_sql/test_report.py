from unittest import TestCase

from pyutil.sql.models import Base, SymbolGroup, Symbol, PortfolioSQL
from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance, reference, history
from pyutil.sql.session import test_session, session_scope
from test.config import test_portfolio


class TestHistory(TestCase):

    def test_strategy_2(self):
        with session_scope(session=test_session(Base=Base)) as session:
            g1 = SymbolGroup(name="A", symbols= [Symbol(bloomberg_symbol=symbol) for symbol in ["A","B","C","D"]])
            g2 = SymbolGroup(name="B", symbols= [Symbol(bloomberg_symbol=symbol) for symbol in ["E","F","G"]])

            session.add_all([g1, g2])
            session.add(PortfolioSQL(portfolio=test_portfolio(), name="test"))

            print(mtd(session))
            print(ytd(session))
            print(mtd(session, names=["test"]))
            print(ytd(session, names=["test"]))

            print(sector(session))
            print(recent(session))
            print(period_returns(session))
            print(performance(session))
            print(reference(session))
            print(history(session))


