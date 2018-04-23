from unittest import TestCase

from pyutil.excel.excel import Excel


class TestExcel(TestCase):
    def test_excel(self):
        e = Excel()
        self.assertIsNotNone(e.book)
