import logging

import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.runner import run, _strategy_update
from pyutil.testing.database import database
from test.config import resource, read


@pytest.fixture()
def db():
    db = database(base=Base)

    # add assets to database
    for asset, data in read(name="price.csv", index_col=0, parse_dates=True, header=0).items():
        symbol = Symbol(name=asset, group=SymbolType.fixed_income)
        symbol.price = data.dropna()
        db.session.add(symbol)

    # add strategies to database
    folder = resource("strat")
    for name, source in strategies(folder):
        s = Strategy(name=name, source=source)
        db.session.add(s)

    db.session.commit()

    yield db
    db.session.close()


class TestRunner(object):
    def test_runner(self, db):
        r = run(strategies=db.session.query(Strategy), connection_str=db.connection)

        assert pytest.approx(r["P1"].nav.sharpe_ratio(), -0.23551923609559777, abs=1e-5)
        assert pytest.approx(r["P2"].nav.sharpe_ratio(), -0.20271329554571058, abs=1e-5)

    def test_strategy_update(self, db):
        logger = logging.getLogger(__name__)
        # thils will be a very fresh update
        name, portfolio = _strategy_update(db.session.query(Strategy).filter_by(name="P1").one().id, connection_str=db.connection, logger=logger, n=10)
        # this will update only the very last few days...
        name, portfolio = _strategy_update(db.session.query(Strategy).filter_by(name="P1").one().id, connection_str=db.connection, logger=logger, n=10)

        assert pytest.approx(portfolio.nav.sharpe_ratio(), -0.23551923609559777, abs=1e-5)
        assert name == "P1"
