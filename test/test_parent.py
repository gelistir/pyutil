from unittest import TestCase
from pyutil.parent import Production


class TestDecorator(TestCase):

    def test_without_db(self):
        with Production() as p:
            self.assertIsNotNone(p.logger)
