from unittest import TestCase

from pyutil.excel.excel import Excel, excelBook


class TestExcel(TestCase):
    def test_excel(self):
        e = Excel()
        self.assertIsNotNone(e.book)

    def test_excel_book(self):
        e = excelBook(title="Peter", subject="Maffay")
        self.assertIsNotNone(e)

        self.assertIsNotNone(e.format_number)
        self.assertIsNotNone(e.format_percent)