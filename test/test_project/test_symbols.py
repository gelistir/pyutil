import pandas as pd
import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.testing.database import database



@pytest.fixture()
def ts():
    return pd.Series(data=[2, 4, 6])


@pytest.fixture()
def symbol(ts):
    s = Symbol(name="A", internal="AAA", group=SymbolType.alternatives)
    s.reference["XXX"] = 10
    s.series["PRICE"] = ts
    return s


@pytest.fixture()
def symbols():
    s1 = Symbol(name="A", group=SymbolType.alternatives, internal="AAA")
    s2 = Symbol(name="B", group=SymbolType.currency, internal="BBB")
    return [s1, s2]


@pytest.fixture()
def session(symbols):
    db = database(base=Base)
    db.session.add_all(symbols)
    db.session.commit()
    yield db.session
    db.session.close()


class TestSymbols(object):
    def test_symbol(self, session, symbols):
        # get all symbols from database
        a = Symbol.products(session=session)
        assert len(a) == 2
        assert set(symbols) == set(a)
        assert Symbol.symbolmap(symbols) == {"A": "Alternatives", "B": "Currency"}

    def test_symbols(self, session, symbols):
        # get only one symbol from database
        a = Symbol.products(session=session, names=["A"])
        assert len(a) == 1
        assert a[0] == symbols[0]

    def test_meta(self, symbol):
        assert symbol.internal == "AAA"
        assert symbol.group == SymbolType.alternatives

    def test_reference_frame(self, symbol):
        frame = Symbol.reference_frame(products=[symbol])
        assert frame.index.name == "symbol"
        assert frame["XXX"][symbol] == 10
        assert frame["Sector"][symbol] == "Alternatives"
        assert frame["Internal"][symbol] == "AAA"
