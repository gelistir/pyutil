from unittest import TestCase

#from pyutil.data import Database
from pyutil.quant.reference import update_reference
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.interfaces.ref import Field, DataType, FieldType

#from pyutil.sql.session import postgresql_db_test

from pyutil.test.aux import read_frame, postgresql_db_test
from test.config import resource


class TestQuant(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, _ = postgresql_db_test(base=Base)

        for asset, data in read_frame(resource("price.csv")).items():
            symbol = Symbol(name=asset, group=SymbolType.fixed_income)
            cls.session.add(symbol)

        field = Field(name="f1", result=DataType.string, type=FieldType.dynamic)
        cls.session.add(field)

        cls.session.commit()


    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_reference(self):
        def f(tickers, fields):
            return read_frame(resource("refdata.csv")).reset_index()

        update_reference(symbols=self.session.query(Symbol), fields=self.session.query(Field), reader=f)

        for symbol in self.session.query(Symbol):
            self.assertEqual(symbol.get_reference("f1"), "A{x}".format(x=symbol.name))

