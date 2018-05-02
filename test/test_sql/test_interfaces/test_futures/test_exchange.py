from unittest import TestCase

from pyutil.sql.interfaces.futures.exchange import Exchange


class TestExchange(TestCase):
    def test_exchange(self):
        # define an exchange
        e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
        self.assertEqual(e.name, "Chicago Mercantile Exchange")
        self.assertEqual(e.exch_code, "CME")