from unittest import TestCase
from pyutil.parent import Production


class TestDecorator(TestCase):
    def test_production(self):
        with Production() as p:
            self.assertIsNotNone(p.logger)
            p.logger.warning("Hello Peter Maffay")

    def test_production_error(self):
        with self.assertRaises(AssertionError):
            with Production() as p:
                raise AssertionError
