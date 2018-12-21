from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt
from test.config import test_portfolio, read_frame

from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t1 = pd.Timestamp("2010-04-24")
t2 = pd.Timestamp("2010-04-25")


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p = Portfolio(name="Maffay")

        cls.x = dict()
        for name in ["A", "B", "C"]:
            cls.x[name] = Symbol(name=name, group=SymbolType.equities)

        for name in ["D","E","F","G"]:
            cls.x[name] = Symbol(name=name, group=SymbolType.fixed_income)


        cls.p.upsert(portfolio=test_portfolio(), symbols=cls.x)

    def test_read_influx(self):
        p1 = self.p.portfolio
        pdt.assert_frame_equal(p1.weights, test_portfolio().weights, check_names=False)
        pdt.assert_frame_equal(p1.prices, test_portfolio().prices, check_names=False)

    def test_nav(self):
        pdt.assert_series_equal(self.p.nav, test_portfolio().nav.series, check_names=False)

    def test_leverage(self):
        pdt.assert_series_equal(self.p.leverage, test_portfolio().leverage, check_names=False)

    def test_upsert(self):
        p = 5*test_portfolio().tail(10)
        self.p.upsert(p, self.x)

        x = self.p.portfolio.weights.tail(12).sum(axis=1)
        self.assertAlmostEqual(x["2015-04-08"], 0.305048, places=5)
        self.assertAlmostEqual(x["2015-04-09"], 1.524054, places=5)

    def test_last(self):
        self.assertEqual(self.p._prices.last_valid_index(), pd.Timestamp("2015-04-22"))

    def test_sector(self):
        pdt.assert_series_equal(self.p.sector(total=False).loc["2015-04-22"], pd.Series(index=["Equities","Fixed Income"], data=[0.135671, 0.173303]), check_names=False)
        pdt.assert_series_equal(self.p.sector(total=True).loc["2015-04-22"], pd.Series(index=["Equities", "Fixed Income", "Total"], data=[0.135671, 0.173303, 0.3089738755]), check_names=False)

    def test_state(self):
        pdt.assert_frame_equal(read_frame(name="state.csv"), self.p.state, check_names=False, check_exact=False, check_less_precise=True, check_dtype=False)

