import pytest

from pyutil.portfolio.portfolio import similar
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.strategy import Strategy, StrategyType
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.runner import run, _strategy_update
from pyutil.testing.database import database

from test.config import resource, test_portfolio


@pytest.fixture(scope="module")
def strategy():
    with open(resource("source.py"), "r") as f:
        Strategy.refresh_mongo()
        s = Strategy(name="Peter", source=f.read(), active=True)
        assert s.portfolio is None
        assert s.last_valid_index is None
        s.portfolio = test_portfolio()
        assert s.source
        return s


@pytest.fixture(scope="module")
def symbols():
    p = test_portfolio()
    return [Symbol(name=a, group=SymbolType.alternatives) for a in p.assets]


@pytest.fixture(scope="module")
def db(strategy, symbols):
    db = database(base=Base)
    db.session.add(strategy)
    db.session.add_all(symbols)
    db.session.commit()

    yield db
    db.session.close()


class TestStrategy(object):
    def test_symbol(self, db):
        assert db.connection
        assert db.session

        # extract all strategies from database
        strategies = Strategy.products(session=db.session)
        #
        x = run(strategies=strategies, connection_str=db.connection)
        assert x["Peter"]
        assert similar(x["Peter"], test_portfolio())

    def test_assets(self, strategy):
        assert strategy.name == "Peter"
        assert strategy.assets == test_portfolio().assets
        assert strategy.last_valid_index == test_portfolio().prices.last_valid_index()
        assert similar(strategy.portfolio, test_portfolio())
        assert similar(strategy.configuration(reader=None).portfolio, test_portfolio())

    def test_reference(self, strategy):
        frame = Strategy.reference_frame(products=[strategy])
        assert frame["active"][strategy]
        assert frame["type"][strategy] == StrategyType.conservative
        assert frame["source"][strategy]

    def test_run(self, db):
        for x in db.session.query(Strategy):
            print("Hello")
            print(x)

        s = db.session.query(Strategy).filter(Strategy.name == "Peter").one()
        name, portfolio = _strategy_update(strategy_id=s.id, connection_str=db.connection)
        assert similar(portfolio, test_portfolio())
        assert name == "Peter"