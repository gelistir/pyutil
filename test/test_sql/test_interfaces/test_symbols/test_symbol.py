import pandas as pd
import pandas.util.testing as pdt
from unittest import TestCase

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
        self.assertIsNone(symbol.price)

        # upsert series
        symbol.price = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(symbol.price, test_portfolio().prices["A"].dropna(), check_names=False)

        # extract the last stamp
        self.assertEqual(symbol.price.last_valid_index(), pd.Timestamp("2015-04-22"))

        # test json
        a = symbol.to_json()
        assert isinstance(a, dict)
        self.assertEqual(a["name"], "A")
        pdt.assert_series_equal(a["Price"], symbol.price)



    def test_upsert(self):
        symbol = Symbol(name="B", group=SymbolType.equities, internal="Peter Maffay")

        symbol.price = pd.Series(index=[1,2], data=[5,8])
        symbol.price = merge(old=symbol.price, new=pd.Series(index=[2,4], data=[9,10]))

        pdt.assert_series_equal(symbol.price, pd.Series(index=[1,2,4], data=[5,9,10]))
