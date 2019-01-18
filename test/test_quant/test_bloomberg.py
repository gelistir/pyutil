import pytest

from pyutil.quant.reference import update_reference
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.interfaces.ref import Field, DataType, FieldType

from pyutil.testing.aux import read_frame, postgresql_db_test
from test.config import resource

@pytest.fixture
def session():
    db = postgresql_db_test(base=Base)

    for asset, data in read_frame(resource("price.csv")).items():
        symbol = Symbol(name=asset, group=SymbolType.fixed_income)
        db.session.add(symbol)

    field = Field(name="f1", result=DataType.string, type=FieldType.dynamic)
    db.session.add(field)
    db.session.commit()

    yield db.session

    db.session.close()


class TestQuant(object):
    def test_reference(self, session):

        def f(tickers, fields):
            return read_frame(resource("refdata.csv")).reset_index()

        update_reference(symbols=session.query(Symbol), fields=session.query(Field), reader=f)

        for symbol in session.query(Symbol):
            assert symbol.get_reference("f1") == "A{x}".format(x=symbol.name)

