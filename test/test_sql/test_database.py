from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.model.ref import Field, DataType
from pyutil.sql.interfaces.symbol import Symbol, SymbolType, Portfolio
from pyutil.sql.container import Portfolios, Assets, state
from test.config import test_portfolio, read_frame


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s1 = Symbol(bloomberg_symbol="A", group=SymbolType.equities, internal="A")
        cls.s2 = Symbol(bloomberg_symbol="B", group=SymbolType.equities, internal="B")
        cls.s3 = Symbol(bloomberg_symbol="C", group=SymbolType.fixed_income, internal="C")

        portfolio = test_portfolio().subportfolio(assets=["A", "B", "C"])
        p = Portfolio(name="Peter").upsert(portfolio=portfolio, assets={"A": cls.s1, "B": cls.s2, "C": cls.s3})

        f1 = Field(name="Field 1", result=DataType.integer)
        f2 = Field(name="Field 2", result=DataType.integer)

        cls.s1.upsert_ref(f1, value="100")
        cls.s1.upsert_ref(f2, value="200")
        cls.s2.upsert_ref(f1, value="10")
        cls.s2.upsert_ref(f2, value="20")
        cls.s3.upsert_ref(f1, value="30")
        cls.s3.upsert_ref(f2, value="40")

        cls.s1.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["A"])
        cls.s2.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["B"])
        cls.s3.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["C"])

        cls.portfolios = Portfolios([p])
        cls.assets = Assets([cls.s1, cls.s2, cls.s3])

    def test_reference(self):
        f = pd.DataFrame(columns=["Field 1", "Field 2"], index=[self.s1, self.s2, self.s3], data=[[100, 200], [10, 20], [30, 40]])
        x = self.assets.reference
        pdt.assert_frame_equal(f, x)

    def test_history(self):
        pdt.assert_frame_equal(self.assets.history().rename(columns= lambda x: x.bloomberg_symbol), read_frame("price.csv")[["A", "B", "C"]], check_names=False)

    def test_ytd(self):
        self.assertAlmostEqual(self.portfolios.ytd["Jan"]["Peter"], 0.007706, places=5)

    def test_mtd(self):
        self.assertAlmostEqual(self.portfolios.mtd["Apr 02"]["Peter"], 0.000838, places=5)

    # def test_sector(self):
    #    self.assertAlmostEqual(self.db.sector["equities"]["Peter"], 0.135671, places=5)

    def test_recent(self):
        self.assertAlmostEqual(self.portfolios.recent(n=10)["Apr 21"]["Peter"], 0.002367, places=5)

    def test_performance(self):
        self.assertAlmostEqual(self.portfolios.performance["Max Nav"]["Peter"], 1.00077, places=5)

    def test_periods(self):
        self.assertAlmostEqual(self.portfolios.period_returns["One Year"]["Peter"], 0.015213, places=5)

    def test_state(self):
        print(state(self.portfolios[0]))

    def test_sector(self):
        print(self.portfolios.sector)

# if __name__ == '__main__':
#     x = TestDatabase()
#     p = x.portfolios
#     f = p.frames
#
#     #r = Report(session=test_session(), portfolio=test_portfolio())
#     #r.state.to_csv("resources/state.csv")
#     #f = frames(session=test_session())
#
#     for key, frame in f.items():
#         frame.to_csv("../resources/{name}.csv".format(name=key))
