import pytest
import pandas.util.testing as pdt

from pyutil.mongo.mongo import client, Collection
from pyutil.sql.interfaces.ref import Field, DataType
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType, SymbolTypes
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture(scope="module")
def collection():
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)
    return c


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

    def test_prices(self, ts, collection):
        symbol = Symbol(name="Thomas", group=SymbolType.alternatives)
        symbol.write_price(collection=collection, data=ts)
        x = symbol.read_price(collection=collection)
        pdt.assert_series_equal(x, ts)

        frame = Symbol.read_prices(collection=collection)
        pdt.assert_series_equal(frame["Thomas"], ts, check_names=False)
