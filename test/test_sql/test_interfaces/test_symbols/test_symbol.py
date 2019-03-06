import pandas as pd
import pandas.util.testing as pdt
from unittest import TestCase

from pyutil.sql.interfaces.ref import Field, DataType
from pyutil.timeseries.merge import merge
from test.config import test_portfolio

from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType


class TestSymbol(TestCase):
    def test_init(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
        self.assertEqual(symbol.internal, "Peter Maffay")
        self.assertEqual(symbol.group, SymbolType.equities)
        self.assertEqual(symbol.discriminator, "symbol")

    def test_ts(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")

        # update with a series containing a NaN
        self.assertIsNone(symbol._price)

        # upsert series
        symbol._price = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(symbol._price, test_portfolio().prices["A"].dropna(), check_names=False)

        # extract the last stamp
        self.assertEqual(symbol._price.last_valid_index(), pd.Timestamp("2015-04-22"))

        # test json
        #a = symbol.to_json()
        #assert isinstance(a, dict)
        #self.assertEqual(a["name"], "A")
        #pdt.assert_series_equal(a["Price"], symbol._price)


    def test_upsert(self):
        symbol = Symbol(name="B", group=SymbolType.equities, internal="Peter Maffay")

        symbol._price = pd.Series(index=[1, 2], data=[5, 8])
        symbol._price = merge(old=symbol._price, new=pd.Series(index=[2, 4], data=[9, 10]))

        pdt.assert_series_equal(symbol._price, pd.Series(index=[1, 2, 4], data=[5, 9, 10]))

    def test_empty_price(self):
        symbol = Symbol(name="C", group=SymbolType.fixed_income)
        self.assertIsNone(symbol._price)

        symbol._price = pd.Series({})
        print(symbol._price)
        pdt.assert_series_equal(symbol._price, pd.Series({}))

        symbol = Symbol(name="D", group=SymbolType.currency)
        symbol._price = None
        self.assertIsNone(symbol._price)

    def test_upsert_price(self):
        symbol = Symbol(name="D", group=SymbolType.currency)
        symbol.upsert_price(ts = pd.Series(index=[pd.Timestamp("2010-04-28").date()], data=[10]))
        pdt.assert_series_equal(symbol.price, pd.Series(index=[pd.Timestamp("2010-04-28").date()], data=[10]))

    def test_frame(self):
        symbol = Symbol(name="E", group=SymbolType.fixed_income)
        field = Field(name="KIID", result=DataType.integer)
        symbol.reference[field] = 2
        print(Symbol.reference_frame([symbol]))
        #todo: finish test
