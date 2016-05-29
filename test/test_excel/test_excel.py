from unittest import TestCase

from pyutil.excel.excel import Excel
import xlsxwriter
import pandas as pd
from tempfile import NamedTemporaryFile
import os


class TestExcel(TestCase):
    def test_init(self):
        x = Excel()
        assert isinstance(x.book, xlsxwriter.workbook.Workbook)

    def test_name(self):
        x = Excel()
        assert x.stream

    def test_write(self):
        x = Excel()
        frame = pd.DataFrame(data=[[2.0, 3.0],[4.0, 5.0]])
        sheet = x.add_frame(frame, sheetname="test")
        sheet.set_column("A:A", 20, x.format_percent)

        f = NamedTemporaryFile(mode="w+b", delete=True)
        x.to_file(f)
        os.path.exists(f.name)
