from unittest import TestCase
from pyutil.parent import Production


class TestDecorator(TestCase):

    def test_production(self):
        with Production(format="%(name)s - %(levelname)s - %(message)s") as p:
            self.assertIsNotNone(p.logger)
            p.logger.warning("Hello Peter Maffay")
            self.assertEqual(p.log_stream, "LWM - WARNING - Hello Peter Maffay\n")

