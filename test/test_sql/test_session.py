from unittest import TestCase

from pyutil.sql.models import _Base, Symbol
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=_Base.metadata)

        x = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        y = get_one_or_create(session, Symbol, bloomberg_symbol="B")

        self.assertEqual(x,y)

    def test_get_one_or_none(self):
        session = session_test(meta=_Base.metadata)
        self.assertIsNone(get_one_or_none(session, Symbol, bloomberg_symbol="C"))

