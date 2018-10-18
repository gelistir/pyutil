import pandas as pd
import pandas.util.testing as pdt
from unittest import TestCase
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
        self.assertIsNone(symbol.last(field="PX_LAST"))

        # upsert series
        symbol.ts["PX_LAST"] = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(symbol.ts["PX_LAST"], test_portfolio().prices["A"].dropna(), check_names=False)

        # extract the last stamp
        self.assertEqual(symbol.last(field="PX_LAST"), pd.Timestamp("2015-04-22"))

    def test_upsert(self):
        symbol = Symbol(name="B", group=SymbolType.equities, internal="Peter Maffay")

        symbol.ts["PX_LAST"] = pd.Series(index=[1,2], data=[5,8])
        symbol.ts["PX_LAST"] = pd.Series(index=[2,4], data=[9,10])

        pdt.assert_series_equal(symbol.ts["PX_LAST"], pd.Series(index=[1,2,4], data=[5,9,10]))
