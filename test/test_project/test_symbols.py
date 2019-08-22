import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.testing.database import database


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
        a = Symbol.products(session=session)
        assert len(a) == 2
        assert set(symbols) == set(a)

    def test_symbolmap(self, symbols):
        b = Symbol.symbolmap(symbols)
        assert b == {"A": "Alternatives", "B": "Currency"}

    def test_symbols(self, session, symbols):
        a = Symbol.products(session=session, names=["A"])
        assert len(a) == 1
        assert a[0] == symbols[0]