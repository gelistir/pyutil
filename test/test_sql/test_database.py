from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.common import DataType
from pyutil.sql.database import Database
from pyutil.sql.models import Symbol, SymbolType, PortfolioSQL
from pyutil.sql.products import Field
from pyutil.sql.session import session_test
from test.config import test_portfolio, read_frame


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        session = session_test(meta=Base.metadata)

        s1 = Symbol(bloomberg_symbol="A", group=SymbolType.equities, internal="A")
        s2 = Symbol(bloomberg_symbol="B", group=SymbolType.equities, internal="B")
        s3 = Symbol(bloomberg_symbol="C", group=SymbolType.fixed_income, internal="C")

        # add all symbols to database
        session.add_all([s1, s2, s3])

        portfolio = test_portfolio().subportfolio(assets=["A", "B", "C"])
        p = PortfolioSQL(name="Peter").upsert(portfolio=portfolio)
        session.add(p)

        f1 = Field(name="Field 1", result=DataType.integer)
        f2 = Field(name="Field 2", result=DataType.integer)
        session.add_all([f1, f2])

        s1.upsert_ref(f1, value="100")
        s1.upsert_ref(f2, value="200")
        s2.upsert_ref(f1, value="10")
        s2.upsert_ref(f2, value="20")
        s3.upsert_ref(f1, value="30")
        s3.upsert_ref(f2, value="40")


        s1.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["A"])
        s2.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["B"])
        s3.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["C"])

        cls.db = Database(session=session)

    def test_reference(self):
        f = pd.DataFrame(columns=["Field 1", "Field 2"], index=["A", "B", "C"], data=[[100, 200], [10, 20], [30, 40]])
        f.index.name = "Asset"

        x = self.db.reference(cls=Symbol)
        x = x.rename(index=lambda x: x.bloomberg_symbol)

        pdt.assert_frame_equal(f, x)


    def test_history(self):
        pdt.assert_frame_equal(self.db.history().rename(columns=lambda x: x.bloomberg_symbol), read_frame("price.csv")[["A", "B", "C"]], check_names=False)

    def test_ytd(self):
        self.assertAlmostEqual(self.db.ytd()["Jan"]["Peter"], 0.007706, places=5)
        self.assertAlmostEqual(self.db.ytd(names=["Peter"])["Jan"]["Peter"], 0.007706, places=5)

    def test_mtd(self):
        self.assertAlmostEqual(self.db.mtd()["Apr 02"]["Peter"], 0.000838, places=5)
        self.assertAlmostEqual(self.db.mtd(names=["Peter"])["Apr 22"]["Peter"], 0.00015664473396981293, places=5)

    def test_sector(self):
        self.assertAlmostEqual(self.db.sector()["equities"]["Peter"], 0.135671, places=5)

    def test_recent(self):
        self.assertAlmostEqual(self.db.recent(n=10)["Apr 21"]["Peter"], 0.002367, places=5)

    def test_performance(self):
        self.assertAlmostEqual(self.db.performance()["Max Nav"]["Peter"], 1.00077, places=5)

    def test_periods(self):
        self.assertAlmostEqual(self.db.period_returns()["One Year"]["Peter"], 0.015213, places=5)
