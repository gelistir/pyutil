from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.model.ref import Field, DataType
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType, Symbols
from pyutil.sql.interfaces.symbols.portfolio import Portfolio, Portfolios
from test.config import test_portfolio, read_frame


class TestContainer(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s1 = Symbol(name="A", group=SymbolType.equities, internal="A")
        cls.s2 = Symbol(name="B", group=SymbolType.equities, internal="B")
        cls.s3 = Symbol(name="C", group=SymbolType.fixed_income, internal="C")

        portfolio = test_portfolio().subportfolio(assets=["A", "B", "C"])
        p = Portfolio(name="Peter").upsert_portfolio(portfolio=portfolio, assets={"A": cls.s1, "B": cls.s2, "C": cls.s3})

        f1 = Field(name="Field 1", result=DataType.integer)
        f2 = Field(name="Field 2", result=DataType.integer)

        cls.s1.reference[f1] = "100"
        cls.s1.reference[f2] = "200"
        cls.s2.reference[f1] = "10"
        cls.s2.reference[f2] = "20"
        cls.s3.reference[f1] = "30"
        cls.s3.reference[f2] = "40"

        cls.s1.upsert_ts(name="PX_LAST", data=read_frame("price.csv")["A"])
        cls.s2.upsert_ts(name="PX_LAST", data=read_frame("price.csv")["B"])
        cls.s3.upsert_ts(name="PX_LAST", data=read_frame("price.csv")["C"])

        cls.portfolios = Portfolios([p])
        cls.assets = Symbols([cls.s1, cls.s2, cls.s3])

    def test_reference(self):
        f = pd.DataFrame(columns=["Field 1", "Field 2"], index=["A", "B", "C"], data=[[100, 200], [10, 20], [30, 40]])
        f.index.names = ["Product"]
        x = self.assets.reference(rename=True)
        pdt.assert_frame_equal(f, x)

    def test_history(self):
        pdt.assert_frame_equal(self.assets.history().rename(columns= lambda x: x.name), read_frame("price.csv")[["A", "B", "C"]], check_names=False)

    def test_ytd(self):
        self.assertAlmostEqual(self.portfolios.ytd["Jan"]["Peter"], 0.007706, places=5)

    def test_mtd(self):
        self.assertAlmostEqual(self.portfolios.mtd["Apr 02"]["Peter"], 0.000838, places=5)

    def test_recent(self):
        self.assertAlmostEqual(self.portfolios.recent(n=10)["Apr 21"]["Peter"], 0.002367, places=5)

    def test_performance(self):
        self.assertAlmostEqual(self.portfolios.performance["Max Nav"]["Peter"], 1.00077, places=5)

    def test_periods(self):
        self.assertAlmostEqual(self.portfolios.period_returns["One Year"]["Peter"], 0.015213, places=5)

    def test_state(self):
        print(self.portfolios["Peter"].state)

    def test_to_dict(self):
        self.assertDictEqual(self.assets.to_dict(), {"A": self.s1, "B": self.s2, "C": self.s3})

    def test_sector(self):
        self.assertAlmostEqual(self.portfolios.sector(total=True)["equities"]["Peter"], 0.135671, places=5)

    def test_frames(self):
        self.assertSetEqual(set(self.portfolios.frames().keys()), {"recent","ytd","mtd","sector","periods","performance"})
