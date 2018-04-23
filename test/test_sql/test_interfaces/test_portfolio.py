import pandas as pd
from unittest import TestCase

from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.symbol import Symbol, SymbolType

import pandas.util.testing as pdt


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s1 = Symbol(bloomberg_symbol="A", group=SymbolType.fixed_income, internal="AA")
        cls.s2 = Symbol(bloomberg_symbol="B", group=SymbolType.equities, internal="BB")

        cls.s1.upsert_ts(name="price", data={2: 10.0})
        cls.s2.upsert_ts(name="price", data={2: 12.0})

        cls.p = Portfolio(name="test")
        cls.p.symbols.append(cls.s1)
        cls.p.symbols.append(cls.s2)

    def test_symbols(self):
        self.assertListEqual(self.p.symbols, [self.s1, self.s2])

    def test_name(self):
        self.assertEqual(self.p.name, "test")

    def test_empty(self):
        # you always have to give the prices again through the portfolio interface!
        self.assertTrue(self.p.empty)
        self.assertTrue(Portfolio(name="test2"))

        self.p.upsert_price(symbol=self.s1, data={pd.Timestamp("2012-05-05"): 10.0, pd.Timestamp("2012-05-07"): 11.0})
        self.p.upsert_weight(symbol=self.s1, data={pd.Timestamp("2012-05-05"): 0.5, pd.Timestamp("2012-05-07"): 0.5})

        pdt.assert_series_equal(self.p.nav, pd.Series({pd.Timestamp("2012-05-05"): 1.0, pd.Timestamp("2012-05-07"): 1.05}))

    def test_portfolio(self):
        self.assertListEqual(self.s1.portfolio, [self.p])

