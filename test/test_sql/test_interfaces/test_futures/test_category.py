from unittest import TestCase

from pyutil.sql.interfaces.futures.category import FuturesCategory


class TestCategory(TestCase):
    def test_category(self):
        # define an exchange
        c = FuturesCategory(name="Equity")
        self.assertEqual(c.name, "Equity")
