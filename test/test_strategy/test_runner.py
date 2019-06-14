import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.runner import run
from pyutil.testing.database import database
from test.config import resource, read


@pytest.fixture()
def db():
    db = database(base=Base)
    yield db
    db.session.close()


@pytest.fixture()
def strats():
    def f(name, source):
        s = Strategy(name=name)
        s.source = source
        return s

    folder = resource("strat")
    return [f(name, source) for name, source in strategies(folder)]


class TestRunner(object):
    def test_runner(self, strats, db):
        # add price data to database
        for asset, data in read(name="price.csv", index_col=0, parse_dates=True, header=0).items():
            symbol = Symbol(name=asset, group=SymbolType.fixed_income)
            symbol.upsert_price(ts=data.dropna())
            db.session.add(symbol)

        # add all strategies to database
        db.session.add_all(strats)
        db.session.commit()

        run(strategies=strats, connection_str=db.connection)

        p1 = db.session.query(Strategy).filter(Strategy.name == "P1").one().portfolio.nav.sharpe_ratio()
        p2 = db.session.query(Strategy).filter(Strategy.name == "P2").one().portfolio.nav.sharpe_ratio()

        assert pytest.approx(p1, -0.23551923609559777, abs=1e-5)
        assert pytest.approx(p2, -0.20271329554571058, abs=1e-5)
