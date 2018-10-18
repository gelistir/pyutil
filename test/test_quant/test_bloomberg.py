from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt
from pyutil.data import Database
from pyutil.quant.history import update_history
from pyutil.quant.reference import update_reference
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import postgresql_db_test

from test.config import resource, read_frame


class TestQuant(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, cls.connection_str = postgresql_db_test(base=Base)

        for asset, data in read_frame(resource("price.csv")).items():
            symbol = Symbol(name=asset)
            cls.session.add(symbol)

        field = Field(name="f1", result=DataType.string, type=FieldType.dynamic)
        cls.session.add(field)

        cls.session.commit()

        # proper database connection!
        cls.data = Database(session=cls.session)

    @classmethod
    def tearDownClass(cls):
        cls.data.close()

    def test_reference(self):
        def f(tickers, fields):
            return read_frame(resource("refdata.csv")).reset_index()

        data = Database(session=self.session)
        update_reference(symbols=data.symbols, fields=data.fields, reader=f)

        for symbol in self.session.query(Symbol):
            self.assertEqual(symbol.get_reference("f1"), "A{x}".format(x=symbol.name))

    def test_history(self):
        def f(tickers, t0=None, field=None):
            return read_frame(resource("price.csv"))[tickers].dropna()

        a = update_history(symbols=self.session.query(Symbol), reader=f, today=pd.Timestamp("2018-01-01"))

        # returns the age here...
        self.assertEqual(a["A"], 985)

        for symbol in self.session.query(Symbol):
            pdt.assert_series_equal(symbol.ts["PX_LAST"], f(symbol.name), check_names=False)


