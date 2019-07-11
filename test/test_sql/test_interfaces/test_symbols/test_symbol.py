import pandas as pd
import pytest
import pandas.util.testing as pdt

from pyutil.mongo.mongo import create_collection
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType, SymbolTypes
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)

@pytest.fixture()
def symbol(ts):
    # point to a new mongo collection...
    ProductInterface.__collection__ = create_collection()
    ProductInterface.__collection_reference__ = create_collection()

    symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
    symbol.write(data=ts, key="PX_OPEN")
    symbol["xxx"] = "A"
    symbol.write(data=ts, key="PX_LAST")

    return symbol


class TestSymbol(object):
    def test_init(self, symbol):
        assert symbol.internal == "Peter Maffay"
        assert symbol.group == SymbolType.equities

    def test_type(self):
        assert "Alternatives" in SymbolTypes.keys()

    def test_prices_1(self, symbol, ts):
        pdt.assert_series_equal(symbol.read(key="PX_OPEN"), ts)

        frame = Symbol.pandas_frame(products=[symbol], key="PX_OPEN")
        pdt.assert_series_equal(frame["A"], ts, check_names=False)

    def test_prices_2(self, symbol, ts):
        pdt.assert_series_equal(symbol.read(key="PX_LAST"), ts)


        symbol.merge(data=2 * ts.tail(100), key="PX_LAST")
        pdt.assert_series_equal(symbol.read(key="PX_LAST").tail(100), 2 * ts.tail(100))

        assert symbol.last(key="PX_LAST") == ts.last_valid_index()

    def test_reference_frame(self, symbol):
        frame = Symbol.reference_frame(symbols=[symbol])

        # print(frame)
        framex = pd.DataFrame(index=[symbol.name], columns=["xxx", "Sector", "Internal"], data=[["A", "Equities", "Peter Maffay"]])
        framex.index.name = "symbol"
        print(frame)

        pdt.assert_frame_equal(frame, framex)
        # assert False
