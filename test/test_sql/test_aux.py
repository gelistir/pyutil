from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.aux import asset, reference, history
from pyutil.sql.models import _Base, Symbol, Field, Timeseries
from pyutil.sql.session import session_test


class TestAux(TestCase):
    def test_reference(self):
        session = session_test(meta=_Base.metadata)
        s1 = Symbol(bloomberg_symbol="A")
        session.add(s1)

        f1 = Field(name="Field 1")
        f2 = Field(name="Field 2")
        session.add_all([f1, f2])

        with session.no_autoflush:
             s1.update_reference(f1, "100")
             s1.update_reference(f2, "200")

        f = pd.DataFrame(columns=["Field 1", "Field 2"], index=["A"], data=[["100", "200"]])
        f.index.name = "Asset"

        pdt.assert_frame_equal(f, reference(session))

    def test_asset(self):
        session = session_test(meta=_Base.metadata)
        s1 = Symbol(bloomberg_symbol="A")
        session.add(s1)
        self.assertEqual(s1, asset(session, name="A"))

    def test_history(self):
        session = session_test(meta=_Base.metadata)

        s1 = Symbol(bloomberg_symbol="C")
        session.add(s1)

        x = pd.Series({pd.Timestamp("2010-01-01").date(): 100.0, pd.Timestamp("2011-01-01").date(): 200.0})
        s1.timeseries["PX_LAST"] = Timeseries(name="PX_LAST").upsert(ts=x)


        pdt.assert_series_equal(s1.timeseries["PX_LAST"].series, x)

        g = x.to_frame("C")
        g.index.name = "Date"
        g.columns.name = "Asset"

        # this is weird...
        g.index = pd.to_datetime(g.index)

        pdt.assert_frame_equal(history(session), g)




