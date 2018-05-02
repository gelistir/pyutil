import pandas as pd
from unittest import TestCase

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.symbols.portfolio import Portfolio, Portfolios
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

import pandas.util.testing as pdt


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s1 = Symbol(bloomberg_symbol="A", group=SymbolType.fixed_income, internal="AA")
        cls.s2 = Symbol(bloomberg_symbol="B", group=SymbolType.equities, internal="BB")

        cls.s1.upsert_ts(name="price", data={pd.Timestamp("2012-05-05"): 10.0})
        cls.s2.upsert_ts(name="price", data={pd.Timestamp("2012-05-05"): 12.0})

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

        prices = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                              data=[[10.0], [11.0]])
        weights = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                               data=[[0.5], [0.5]])
        p = _Portfolio(prices=prices, weights=weights)

        self.p.upsert_portfolio(portfolio=p)
        pdt.assert_series_equal(self.p.nav,
                                pd.Series({pd.Timestamp("2012-05-05"): 1.0, pd.Timestamp("2012-05-07"): 1.05}))

    def test_portfolio(self):
        self.assertListEqual(self.s1.portfolio, [self.p])

    def test_sector(self):
        print(self.p.sector(total=True))

        prices = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                              data=[[10.0], [11.0]])
        weights = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                               data=[[0.5], [0.5]])
        p = _Portfolio(prices=prices, weights=weights)

        self.p.upsert_portfolio(portfolio=p)
        #print(self.p.sector())
        #print(self.p.sector_tail())
        pp = Portfolios([self.p])
        print(pp.sector(total=False))

        for x in pp:
            print(x)
        # todo: fill up

    def test_lt(self):
        p1 = Portfolio(name="A")
        p2 = Portfolio(name="B")
        self.assertTrue(p1 < p2)

    def test_leverage(self):
        prices = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                              data=[[10.0], [11.0]])
        weights = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[self.s1],
                               data=[[0.5], [0.5]])
        p = _Portfolio(prices=prices, weights=weights)
        pdt.assert_series_equal(p.leverage,
                                pd.Series({pd.Timestamp("2012-05-05"): 0.5, pd.Timestamp("2012-05-07"): 0.5}))
