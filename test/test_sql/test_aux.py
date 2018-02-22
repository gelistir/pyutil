from unittest import TestCase

import pandas as pd

from pyutil.sql.aux import asset, upsert_frame
from pyutil.sql.models import Base, Symbol, Frame
from pyutil.sql.session import session_test, session_scope
import pandas.util.testing as pdt

class TestAux(TestCase):

    def test_asset(self):
        with session_scope(session=session_test(meta=Base.metadata)) as session:
            s = Symbol(bloomberg_symbol="A", timeseries=["peter","maffay"])
            session.add(s)

            x = asset(session=session, name="A")
            self.assertSetEqual(set(x.timeseries.keys()), set(["peter","maffay"]))

    def test_upsert_frame(self):
        with session_scope(session=session_test(meta=Base.metadata)) as session:

            x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
            x.index.names = ["index"]

            upsert_frame(session=session, name="peter", frame=x)
            pdt.assert_frame_equal(session.query(Frame).filter(Frame.name=="peter").first().frame, x)


            y = pd.DataFrame(data=[[2.2, 2.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
            y.index.names = ["index"]

            upsert_frame(session=session, name="peter", frame=y)
            pdt.assert_frame_equal(session.query(Frame).filter(Frame.name=="peter").first().frame, y)