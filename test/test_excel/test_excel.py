from unittest import TestCase

from pyutil.excel.excel import Excel
import xlsxwriter
import pandas as pd


class TestExcel(TestCase):
    def test_init(self):
        x = Excel()
        assert isinstance(x.book, xlsxwriter.workbook.Workbook)
        x.save()

    def test_name(self):
        x = Excel()
        assert x.save().stream

    def test_write(self):
        x = Excel()
        frame = pd.DataFrame(data=[[2.0, 3.0],[4.0, 5.0]])
        sheet = x.add_frame(frame, sheetname="test")
        sheet.set_column("A:A", 20, x.format_percent)
        x.save().stream
