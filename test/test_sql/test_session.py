from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.models import Symbol
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=Base.metadata)

        x, exists = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        self.assertFalse(exists)

        y, exists = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        self.assertTrue(exists)

        self.assertEqual(x, y)

    def test_get_one_or_none(self):
        session = session_test(meta=Base.metadata)
        self.assertIsNone(get_one_or_none(session, Symbol, bloomberg_symbol="C"))
