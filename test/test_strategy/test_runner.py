import logging

import pytest

from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.runner import run, _strategy_update
from test.config import resource, read, db


@pytest.fixture(scope="function")
def database(db):
    # check if database is really empty
    assert db.session.query(Strategy).count() == 0
    assert db.session.query(Symbol).count() == 0

    # add assets to database
    for asset, data in read(name="price.csv", index_col=0, parse_dates=True, header=0).items():
        symbol = Symbol(name=asset, group=SymbolType.fixed_income)
        symbol.series["PX_LAST"] = data.dropna()
        db.session.add(symbol)

    # add strategies to database
    folder = resource("strat")
    for name, source in strategies(folder):
        s = Strategy(name=name, source=source)
        db.session.add(s)
        assert s.portfolio is None

    db.session.commit()

    yield db

    db.session.close()


class TestRunner(object):
    def test_runner(self, database):
        r = run(strategies=database.session.query(Strategy), connection_str=database.connection)

        assert pytest.approx(r["P1"].nav.sharpe_ratio(), -0.23551923609559777, abs=1e-5)
        assert pytest.approx(r["P2"].nav.sharpe_ratio(), -0.20271329554571058, abs=1e-5)

    def test_strategy_update(self, database):
        logger = logging.getLogger(__name__)
        # check that the strategy does not contain any portfolio yet
        strategy = database.session.query(Strategy).filter_by(name="P1").one()
        assert strategy.last_valid_index is None

        # thils will be a very fresh update
        name, portfolio = _strategy_update(database.session.query(Strategy).filter_by(name="P1").one().id,
                                           connection_str=database.connection, logger=logger, n=10)
        # this will update only the very last few days...
        name, portfolio = _strategy_update(database.session.query(Strategy).filter_by(name="P1").one().id,
                                           connection_str=database.connection, logger=logger, n=10)

        assert pytest.approx(portfolio.nav.sharpe_ratio(), -0.23551923609559777, abs=1e-5)
        assert name == "P1"
