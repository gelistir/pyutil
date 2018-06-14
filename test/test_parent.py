from unittest import TestCase
from pyutil.parent import Production


class TestDecorator(TestCase):

    def test_production(self):
        with Production(format="%(name)s - %(levelname)s - %(message)s") as p:
            self.assertIsNotNone(p.logger)
            p.logger.warning("Hello Peter Maffay")
            self.assertEqual(p.log_stream, "LWM - WARNING - Hello Peter Maffay\n")

    def test_production_error(self):
        with self.assertRaises(AssertionError):
            with Production(format="%(name)s - %(levelname)s - %(message)s") as p:
                raise AssertionError

        self.assertEqual(p.log_stream[:13], "LWM - ERROR -")