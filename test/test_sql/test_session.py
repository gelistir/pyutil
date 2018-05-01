from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none
from pyutil.sql.interfaces.symbol import Symbol
from pyutil.sql.interfaces.risk.currency import Currency


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=Base.metadata, echo=True)

        x, exists = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        self.assertFalse(exists)

        y, exists = get_one_or_create(session, Symbol, bloomberg_symbol="B")
        self.assertTrue(exists)

        self.assertEqual(x, y)

    def test_get_one_or_none(self):
        session = session_test(meta=Base.metadata, echo=True)
        self.assertIsNone(get_one_or_none(session, Symbol, bloomberg_symbol="C"))

    def test_get_one_or_create_currency(self):
        session = session_test(meta=Base.metadata, echo=False)

        # this works because name is a hybrid property of Currency!
        x, exists = get_one_or_create(session, Currency, name="CHF")
        self.assertFalse(exists)

        y, exists = get_one_or_create(session, Currency, name="CHF")
        self.assertTrue(exists)
