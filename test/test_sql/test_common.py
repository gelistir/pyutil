from unittest import TestCase

from pyutil.sql.common import DataType


class TestCommon(TestCase):
    def test_field(self):
            d = DataType.integer
            self.assertEqual(d("100"), 100)
            self.assertEqual(d.value, "integer")


