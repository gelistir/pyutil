from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, SymbolGroup, Field, PortfolioSQL, Base, Strategy, FieldType
from pyutil.sql.session import session_test
from test.config import read_frame, test_portfolio, resource


class TestHistory(TestCase):
    def test_series(self):
        s = Symbol(bloomberg_symbol="A")
        s.upsert_timeseries(name="PX_LAST", ts=read_frame("price.csv")["A"].dropna())

        t = s._timeseries["PX_LAST"]
        pdt.assert_series_equal(t.series, read_frame("price.csv")["A"].dropna(), check_names=False)

        self.assertFalse(t.empty)
        self.assertEqual(t.last_valid.date(), pd.Timestamp("2015-04-22").date())

        t.upsert(pd.Series(index=[pd.Timestamp("2015-04-22"), pd.Timestamp("2015-04-23")], data=[200, 300]))

    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
        x.index.names = ["index"]
        f = Frame(frame=x, name="test")
        pdt.assert_frame_equal(f.frame, x)

    # def test_timeseries(self):
    # g1 = SymbolGroup(name="A")
    # xx = Symbol(bloomberg_symbol="XX", group=g1)

    # ts = xx.timeseries["hans"]
    # self.assertEqual(str(ts), "hans for Symbol: XX, Group: A")
    # self.assertIsNone(ts.last_valid)

    def test_portfolio(self):
        portfolio = test_portfolio()

        p = PortfolioSQL(name="test")
        p.upsert(portfolio=portfolio)
        pdt.assert_frame_equal(p.price, test_portfolio().prices)
        pdt.assert_frame_equal(p.weight, test_portfolio().weights)
        self.assertEqual(p.assets, test_portfolio().assets)
        self.assertEqual(p.last_valid, test_portfolio().index[-1])

        # test the truncation
        p1 = portfolio.truncate(after=pd.Timestamp("2015-01-01") - pd.DateOffset(seconds=1))
        pp = PortfolioSQL(name="wurst")

        pp.upsert(portfolio=test_portfolio().truncate(after=pd.Timestamp("2015-02-01")))
        self.assertEqual(p1.index[-1], pd.Timestamp("2014-12-31"))
        p2 = portfolio.truncate(before=pd.Timestamp("2015-01-01").date())

        pp.upsert(portfolio=p2)
        pdt.assert_frame_equal(pp.weight, test_portfolio().weights)

    def test_group(self):
        g1 = SymbolGroup(name="Group A", symbols=[Symbol(bloomberg_symbol="A"), Symbol(bloomberg_symbol="B")])
        self.assertEqual(g1.symbols[0].bloomberg_symbol, "A")
        self.assertEqual(g1.symbols[1].bloomberg_symbol, "B")

    #def test_field(self):
    #    session = session_test(meta=Base.metadata)

    #    t1 = Field(name="Name", type=MyType.static)
    #    t2 = Field(name="CHG_PCT_1D", type=MyType.dynamic)
    #    t3 = Type(name="user-defined", fields=[Field(name="REGION")])

    #    session.add_all([t1, t2, t3])

    #    self.assertEqual(session.query(Field).join(Type).filter(Type.name.in_(["BB-static", "BB-dynamic"])).count(), 2)

    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)
            self.assertListEqual(s.assets, [])
            print(s.source)

            s.upsert(portfolio=s.compute_portfolio(reader=None))
            self.assertIsNotNone(s.portfolio)

            pdt.assert_frame_equal(s.portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s.portfolio.price, test_portfolio().prices)

            self.assertEqual(s.portfolio.last_valid, pd.Timestamp("2015-04-22"))
            self.assertListEqual(s.assets, ['A', 'B', 'C', 'D', 'E', 'F', 'G'])
