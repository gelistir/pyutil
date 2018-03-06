from unittest import TestCase

from pyutil.sql.aux import asset
from pyutil.sql.models import Base, Symbol
from pyutil.sql.session import session_test, session_scope


class TestAux(TestCase):

    def test_asset(self):
        with session_scope(session=session_test(meta=Base.metadata)) as session:
            s = Symbol(bloomberg_symbol="A", timeseries=["peter","maffay"])
            session.add(s)

            x = asset(session=session, name="A")
            self.assertSetEqual(set(x.timeseries.keys()), set(["peter","maffay"]))
