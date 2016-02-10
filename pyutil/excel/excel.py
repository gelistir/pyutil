import pandas as pd
import tempfile


class Excel(object):
    """
    Interaction with Excel
    """
    def __init__(self, date_format='dd/mm'):
        self.__tmpData = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False, prefix="lwm_")
        self.__writer = pd.ExcelWriter(self.__tmpData.name, date_format=date_format)
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

    def close(self):
        return self.__writer.close()

    @property
    def format_percent(self):
        return self.__format_percent
