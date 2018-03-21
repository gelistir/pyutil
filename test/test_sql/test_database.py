from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.database import Database
from pyutil.sql.models import _Base, Symbol, Field, Timeseries
from pyutil.sql.session import session_test


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        session = session_test(meta=_Base.metadata)
        s1 = Symbol(bloomberg_symbol="A")
        session.add(s1)

        f1 = Field(name="Field 1")
        f2 = Field(name="Field 2")
        session.add_all([f1, f2])

        with session.no_autoflush:
             s1.refdata[f1] = "100"
             s1.refdata[f2] = "200"

        x = pd.Series({pd.Timestamp("2010-01-01").date(): 100.0, pd.Timestamp("2011-01-01").date(): 200.0})
        s1.timeseries["PX_LAST"] = Timeseries(name="PX_LAST").upsert(ts=x)

        cls.db = Database(session=session)

    def test_reference(self):
        f = pd.DataFrame(columns=["Field 1", "Field 2"], index=["A"], data=[["100", "200"]])
        f.index.name = "Asset"
        pdt.assert_frame_equal(f, self.db.reference())

    def test_asset(self):
        self.assertEqual(Symbol(bloomberg_symbol="A"), self.db.asset(name="A"))

    def test_history(self):
        x = pd.Series({pd.Timestamp("2010-01-01").date(): 100.0, pd.Timestamp("2011-01-01").date(): 200.0})
        pdt.assert_series_equal(self.db.asset(name="A").timeseries["PX_LAST"].series, x)

        g = x.to_frame("A")
        g.index.name = "Date"
        g.columns.name = "Asset"

        # this is weird...
        g.index = pd.to_datetime(g.index)

        pdt.assert_frame_equal(self.db.history(), g)




