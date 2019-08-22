import pandas as pd
import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.testing.database import database

import pandas.util.testing as pdt


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

    def test_lt(self):
        assert Symbol(name="A") < Symbol(name="B")

    def test_name(self, symbol):
        assert str(symbol) == "A"
        assert symbol.internal == "AAA"
        assert symbol.group == SymbolType.alternatives

        # can't change the name of an asset!
        with pytest.raises(AttributeError):
            symbol.name = "AA"

    def test_reference(self, symbol):
        assert symbol.reference["AHAH"] is None
        assert symbol.reference.get(item="NoNoNo", default=5) == 5
        assert symbol.reference["XXX"] == 10
        assert symbol.reference.keys() == {"XXX"}
        assert symbol.reference.collection
        assert {k: v for k, v in symbol.reference.items()} == {"XXX": 10}

    def test_reference_frame(self, symbol):
        frame = Symbol.reference_frame(products=[symbol])
        assert frame.index.name == "symbol"
        assert frame["XXX"][symbol] == 10
        assert frame["Sector"][symbol] == "Alternatives"
        assert frame["Internal"][symbol] == "AAA"

    def test_timeseries(self, symbol, ts):
        pdt.assert_series_equal(symbol.series["PRICE"], ts)

    def test_last(self, symbol, ts):
        assert symbol.series.last(key="PRICE") == ts.last_valid_index()
        assert symbol.series.last(key="OPEN") is None

    def test_merge(self, symbol):
        symbol.series.merge(key="PRICE", data=pd.Series(index=[2,3], data=[10, 20]))
        pdt.assert_series_equal(symbol.series["PRICE"], pd.Series(data=[2,4,10,20]))

    def test_pandas_frame(self, symbol, ts):
        frame = Symbol.pandas_frame(key="PRICE", products=[symbol])
        pdt.assert_series_equal(frame[symbol], ts, check_names=False)