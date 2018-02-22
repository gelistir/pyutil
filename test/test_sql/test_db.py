from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, SymbolGroup, Timeseries, TimeseriesData, Type, Field, \
    SymbolReference, PortfolioSQL
from test.config import read_frame, test_portfolio


class TestHistory(TestCase):
    def test_series(self):

        prices = read_frame("price.csv")
        g = SymbolGroup(name="A")
        s = Symbol(bloomberg_symbol="A", group=g, timeseries=["PX_LAST"])
        t = Timeseries(name="PX_LAST", symbol=s)
        t.upsert(read_frame("price.csv")["A"].dropna())



        #g = SymbolGroup(name="A", symbols=[Symbol(bloomberg_symbol="A", timeseries={"PX_LAST": Timeseries(name="PX_LAST")})])
        #g.symbols[0].timeseries["PX_LAST"].data = {date : TimeseriesData(date=date, value=value) for date, value in prices["A"].dropna().items()}

        #s = g.symbols[0].timeseries["PX_LAST"]

        pdt.assert_series_equal(s.timeseries["PX_LAST"].series, read_frame("price.csv")["A"].dropna(), check_names=False)

        self.assertFalse(t.empty)
        self.assertEqual(t.last_valid.date(), pd.Timestamp("2015-04-22").date())

        t.upsert(pd.Series(index=[pd.Timestamp("2015-04-22"), pd.Timestamp("2015-04-23")], data=[200, 300]))



    def test_ref(self):
        t1 = Type(name="BB-static", fields= [Field(name="Name")])
        t2 = Type(name="BB-dynamic", fields=[Field(name="CHG_PCT_1D")])
        t3 = Type(name="user-defined", fields=[Field(name="REGION")])
        self.assertEqual(str(t1), "Type: BB-static")

        g1 = SymbolGroup(name="A") #, symbols=[Symbol(bloomberg_symbol="XX")])
        g2 = SymbolGroup(name="B") #, symbols=[Symbol(bloomberg_symbol="YY")])
        self.assertEqual(str(g1), "Group: A")

        s1 = Symbol(bloomberg_symbol="XX", group=g1)
        s2 = Symbol(bloomberg_symbol="YY", group=g2)


        name = t1.fields[0]
        chg = t2.fields[0]
        region = t3.fields[0]
        self.assertEqual(str(name), "Field: Name, Type: BB-static")

        xx = g1.symbols[0]
        yy = g2.symbols[0]
        self.assertEqual(str(xx), "Symbol: XX, Group: A")


        # either
        xx.update_reference(field=name, value="Hans")
        # or
        SymbolReference(field=region, symbol=xx, content="Europe")
        SymbolReference(field=chg, symbol=xx, content="0.40")
        SymbolReference(field=name, symbol=yy, content="Urs")
        SymbolReference(field=region, symbol=yy, content="Europe")
        ref = SymbolReference(field=chg, symbol=yy, content="0.40")
        self.assertEqual(str(ref), "Symbol: YY, Group: B, Field: CHG_PCT_1D, Type: BB-dynamic, Value: 0.40")
        self.assertEqual(xx.reference["REGION"], "Europe")

        g1.symbols[0].update_reference(t1.fields[0], "Wurst")
        self.assertEqual(xx.reference["Name"], "Wurst")

    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A","B"], columns=["X1", "X2"])
        x.index.names = ["index"]
        f = Frame(frame=x, name="test")
        pdt.assert_frame_equal(f.frame, x)

    def test_timeseries(self):
        g1 = SymbolGroup(name="A")
        xx = Symbol(bloomberg_symbol="XX", group=g1, timeseries=["hans", "wurst", "dampf"])

        ts = xx.timeseries["hans"]
        self.assertEqual(str(ts), "hans for Symbol: XX, Group: A")
        self.assertIsNone(ts.last_valid)

    def test_portfolio(self):
        portfolio = test_portfolio()
        strategy = "strat"

        p = PortfolioSQL(portfolio=portfolio, name="test", strategy=strategy)
        pdt.assert_frame_equal(p.price, test_portfolio().prices)
        pdt.assert_frame_equal(p.weight, test_portfolio().weights)
        self.assertEqual(p.assets, test_portfolio().assets)
        self.assertEqual(p.last_valid, test_portfolio().index[-1])

        # test the truncation
        p1 = portfolio.truncate(after=pd.Timestamp("2015-01-01") - pd.DateOffset(seconds=1))
        pp = PortfolioSQL(portfolio=test_portfolio().truncate(after=pd.Timestamp("2015-02-01")), name="wurst")

        self.assertEqual(p1.index[-1], pd.Timestamp("2014-12-31"))
        p2 = portfolio.truncate(before=pd.Timestamp("2015-01-01").date())

        pp.upsert(portfolio=p2)
        pdt.assert_frame_equal(pp.weight, test_portfolio().weights)


    def test_group(self):
        g1 = SymbolGroup(name="Group A", symbols=[Symbol(bloomberg_symbol="A"), Symbol(bloomberg_symbol="B")])
        print(g1.symbols[0])
        print(g1.symbols[1])

        assert False
