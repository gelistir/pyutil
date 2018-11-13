from unittest import TestCase
from pyutil.parent import Production
from pyutil.logconf import logger


class TestDecorator(TestCase):
    def test_production(self):
        with Production(log=logger) as p:
            self.assertIsNotNone(p.logger)
            p.logger.warning("Hello Peter Maffay")
            self.assertTrue("WARNING - Hello Peter Maffay" in p.logger.value())

    def test_production_error(self):
        with self.assertRaises(AssertionError):
            with Production(log=logger) as p:
                raise AssertionError
