from unittest import TestCase

from sqlalchemy.orm import sessionmaker, Session

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test_2
from test.config import test_portfolio

class TestAux(TestCase):
    def test_StratRunner(self):
        engine = postgresql_db_test_2(base=Base, echo=False)
        session = Session(bind=engine.connect())

        #factory = sessionmaker(bind=engine)
        #session = factory()

        for asset, data in test_portfolio().prices.items():
            s = Symbol(name=asset)
            s.upsert(field="PX_LAST", ts=data.dropna())
            session.add(s)

        session.commit()

        #runner = StrategyRunner(engine=engine)
        #runner.upsert_strategies(folder=resource("strat"))
        #runner.run()
