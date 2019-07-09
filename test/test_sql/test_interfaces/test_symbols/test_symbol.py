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

# point to a new mongo collection...
ProductInterface.__collection__ = create_collection()

class TestSymbol(object):
    def test_init(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
        assert symbol.internal == "Peter Maffay"
        assert symbol.group == SymbolType.equities
        assert symbol.discriminator == "symbol"
        assert symbol.__tablename__ == "symbol"
        assert symbol.__mapper_args__ == {'polymorphic_identity': 'symbol'}

    def test_type(self):
        assert "Alternatives" in SymbolTypes.keys()

    def test_prices_1(self, ts):
        symbol = Symbol(name="Thomas")
        symbol.write(data=ts, kind="PX_OPEN")
        pdt.assert_series_equal(symbol.read(kind="PX_OPEN"), ts)

        frame = Symbol.frame(kind="PX_OPEN")
        pdt.assert_series_equal(frame["Thomas"], ts, check_names=False)

    def test_prices_2(self, ts):
        symbol = Symbol(name="Peter Maffay")
        symbol.upsert_price(data=ts)
        pdt.assert_series_equal(symbol.price, ts)

        symbol.price = ts
        pdt.assert_series_equal(symbol.price, ts)

        symbol.upsert_price(data=2*ts.tail(100))
        pdt.assert_series_equal(symbol.price.tail(100), 2*ts.tail(100))

        assert symbol.last == ts.last_valid_index()

    def test_prices_3(self, ts):
        s1 = Symbol(name="A")
        s1.price = ts

        s2 = Symbol(name="B")
        s2.price = 2*ts

        f = Symbol.prices([s1, s2])
        pdt.assert_series_equal(f["A"], ts, check_names=False)
        pdt.assert_series_equal(f["B"], 2*ts, check_names=False)

    def test_reference_frame(self):
        s = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
        frame = Symbol.reference_frame(symbols=[s])
        #print(frame)
        framex = pd.DataFrame(index=[s], columns=["Sector", "Internal"], data=[["Equities", "Peter Maffay"]])
        framex.index.name = "symbol"
        pdt.assert_frame_equal(frame, framex)
        #assert False