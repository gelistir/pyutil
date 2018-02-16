from unittest import TestCase

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyutil.sql.models import Base, SymbolGroup, Symbol, PortfolioSQL
from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance, reference, history
from test.config import test_portfolio


class TestHistory(TestCase):

    @staticmethod
    def session():
        #with TestEnv() as env:
        engine = create_engine("sqlite://")

        # make the tables...
        Base.metadata.create_all(engine)

        return sessionmaker(bind=engine)()


    def test_strategy_2(self):
        session = self.session()
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
        #assert False

