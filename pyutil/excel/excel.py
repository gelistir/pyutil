# Safe import for either Python 2.x or 3.x
try:
    from io import BytesIO
except ImportError:
    from cStringIO import StringIO as BytesIO

import pandas as pd


class Excel(object):
    """
    Interaction with Excel
    """
    def __init__(self, date_format='dd/mm'):
        self.__bio = BytesIO()
        self.__writer = pd.ExcelWriter(self.__bio, engine='xlsxwriter', date_format=date_format)
        self.__format_percent = self.add_format({'num_format': '#.##%'})

    @property
    def book(self):
        return self.__writer.book

    def add_format(self, format):
        return self.__writer.book.add_format(format)

    def add_frame(self, frame, sheetname):
        frame.to_excel(self.__writer, sheet_name=sheetname)
        return self.__writer.sheets[sheetname]

    def save(self):
        self.__writer.save()
        return self

    @property
    def stream(self):
        self.__bio.seek(0)
        return self.__bio.read()

    @property
    def format_percent(self):
        return self.__format_percent

    def to_file(self, file):
        with open(file, mode="w+b") as f:
            f.write(self.stream)

