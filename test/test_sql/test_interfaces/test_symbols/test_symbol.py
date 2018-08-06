import pandas as pd
import pandas.util.testing as pdt
from unittest import TestCase

from test.config import test_portfolio

from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType


class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        Symbol.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        Symbol.client.close()

    def test_init(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")
        self.assertEqual(symbol.internal, "Peter Maffay")
        self.assertEqual(symbol.group, SymbolType.equities)
        self.assertEqual(symbol.discriminator, "symbol")

    def test_ts(self):
        symbol = Symbol(name="A", group=SymbolType.equities, internal="Peter Maffay")

        # update with a series containing a NaN
        self.assertIsNone(symbol.last())

        # upsert series
        symbol.upsert(ts=test_portfolio().prices["A"])

        # extract the series again
        pdt.assert_series_equal(symbol.price(), test_portfolio().prices["A"].dropna(), check_names=False)

        # extract the last stamp
        self.assertEqual(symbol.last(), pd.Timestamp("2015-04-22"))

        # call a static method
        pdt.assert_series_equal(Symbol.symbol(name="A"), symbol.price())

        pdt.assert_series_equal(Symbol.frame()["A"], symbol.price(), check_names=False)




