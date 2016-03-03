# Safe import for either Python 2.x or 3.x
try:
    from io import BytesIO
except ImportError:
    from cStringIO import StringIO as BytesIO

import pandas as pd
from pyutil.config import mail


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

    @property
    def name(self):
        return self.__tmpData.name

    def add_format(self, format):
        return self.book.add_format(format)

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


if __name__ == '__main__':

    df = pd.DataFrame(data=[[2.0,3.0]])
    df2 = pd.DataFrame(data=[[4.0,3.0]])

    e = Excel()
    e.add_frame(df, sheetname="sheet1")
    e.add_frame(df2, sheetname="sheet2")

    m = mail()
    m.toAdr = "thomas.schmelzer@gmail.com"
    m.fromAdr = "mm@lobnek.com"
    m.attach_stream("hans.xlsx", e.save().stream)
    m.send(text="hans wurst")

    print(e.stream)
    print(e.stream)
