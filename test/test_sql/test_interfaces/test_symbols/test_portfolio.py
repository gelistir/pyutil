import pandas as pd
from unittest import TestCase

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

import pandas.util.testing as pdt


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s1 = Symbol(name="A", group=SymbolType.fixed_income, internal="AA")
        cls.s2 = Symbol(name="B", group=SymbolType.equities, internal="BB")

        cls.s1.upsert_ts(name="price", data={pd.Timestamp("2012-05-05"): 10.0})
        cls.s2.upsert_ts(name="price", data={pd.Timestamp("2012-05-05"): 12.0})

        cls.p = Portfolio(name="test")

        prices = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[cls.s1],
                              data=[[10.0], [11.0]])

        weights = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=[cls.s1],
                               data=[[0.5], [0.5]])

        p = _Portfolio(prices=prices, weights=weights)

        cls.p.upsert_portfolio(portfolio=p)

    def test_empty(self):
        # you always have to give the prices again through the portfolio interface!
        self.assertFalse(self.p.empty)

    def test_nav(self):
        pdt.assert_series_equal(self.p.nav, pd.Series({pd.Timestamp("2012-05-05"): 1.0, pd.Timestamp("2012-05-07"): 1.05}))

    def test_leverage(self):
        pdt.assert_series_equal(self.p.leverage, pd.Series({pd.Timestamp("2012-05-05"): 0.5, pd.Timestamp("2012-05-07"): 0.5}))

    def test_portfolio_link(self):
        self.assertListEqual(self.s1.portfolio, [self.p])

    def test_sector(self):
        pdt.assert_series_equal(self.p.sector_tail(total=True), pd.Series({"fixed_income": 0.5, "total": 0.5}))

    def test_upsert(self):
        prices = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=["A"],
                              data=[[10.0], [11.0]])

        weights = pd.DataFrame(index=[pd.Timestamp("2012-05-05"), pd.Timestamp("2012-05-07")], columns=["A"],
                               data=[[0.5], [0.5]])

        p = _Portfolio(prices=prices, weights=weights)

        x = Portfolio(name="Peter")

        # if the portfolio is not based on Symbol objects, you need to give them explicitly
        with self.assertRaises(AssertionError):
            x.upsert_portfolio(portfolio=p)

        # you can avoid the problem by using
        x.upsert_portfolio(portfolio=p, assets={"A": Symbol(name="A")})
