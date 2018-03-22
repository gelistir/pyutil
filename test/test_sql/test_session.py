from unittest import TestCase

import pandas as pd

from pyutil.sql.models import _Base, Symbol, _Timeseries
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none

import pandas.util.testing as pdt


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=_Base.metadata)

        x = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        y = get_one_or_create(session, Symbol, bloomberg_symbol="B")

        self.assertEqual(x,y)

    def test_get_one_or_none(self):
        session = session_test(meta=_Base.metadata)
        self.assertIsNone(get_one_or_none(session, Symbol, bloomberg_symbol="C"))

    def test_timeseries_1(self):
        session = session_test(meta=_Base.metadata)

        s = Symbol(bloomberg_symbol="A")
        session.add(s)

        s.timeseries["wurst"] = {pd.Timestamp("2010-01-01").date(): 2.0, pd.Timestamp("2010-01-02").date(): 3.0, pd.Timestamp("2010-01-04").date(): 5.0}
        session.commit()

        with self.assertRaises(AttributeError):
            s.timeseries["wurst"] = {pd.Timestamp("2010-01-02").date(): 10.0, pd.Timestamp("2010-01-05").date(): 10.0}
            session.commit()

        pdt.assert_series_equal(s.timeseries["wurst"], pd.Series({pd.Timestamp("2010-01-01").date(): 2.0, pd.Timestamp("2010-01-02").date(): 3.0, pd.Timestamp("2010-01-04").date(): 5.0}))

    def test_timeseries_2(self):
        session = session_test(meta=_Base.metadata)
        t = _Timeseries(name="Peter")
        session.add(t)

        t.upsert(ts={pd.Timestamp("2010-01-02").date(): 10.0, pd.Timestamp("2010-01-05").date(): 10.0})
        session.commit()
        print(t.series)

        t.upsert(ts={pd.Timestamp("2010-01-05").date(): 20.0, pd.Timestamp("2010-01-07").date(): 20.0})
        session.commit()

        print(t.series)
        #assert False
