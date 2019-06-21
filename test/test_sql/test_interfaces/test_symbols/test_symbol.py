import pytest
import pandas.util.testing as pdt


from pyutil.sql.interfaces.ref import Field, DataType
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType, SymbolTypes
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestSymbol(object):
    def test_init(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
        assert symbol.internal == "Peter Maffay"
        assert symbol.group == SymbolType.equities
        assert symbol.discriminator == "symbol"
        assert symbol.__tablename__ == "symbol"
        assert symbol.__mapper_args__ == {'polymorphic_identity': 'symbol'}

    def test_frame(self):
        symbol = Symbol(name="E", group=SymbolType.fixed_income)
        field = Field(name="KIID", result=DataType.integer)
        symbol.reference[field] = 2
        print(Symbol.reference_frame([symbol]))
        #todo: finish test

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
