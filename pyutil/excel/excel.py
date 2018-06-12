from io import BytesIO

import pandas as pd


class Excel(object):
    """
    Interaction with Excel
    """
    def __init__(self, date_format='dd/mm', **kwargs):
        self.__bio = BytesIO()
        self.__writer = pd.ExcelWriter(self.__bio, engine='xlsxwriter', date_format=date_format, **kwargs)
        self.__format_percent = self.add_format({'num_format': '#.##%'})
        self.__format_number = self.add_format({'num_format': '#.##'})

    @property
    def book(self):
        return self.__writer.book

    def add_format(self, format):
        return self.__writer.book.add_format(format)

    def add_frame(self, frame, sheetname):
        frame.to_excel(self.__writer, sheet_name=sheetname)
        return self.__writer.sheets[sheetname]

    def __save(self):
        self.__writer.save()
        return self

    @property
    def stream(self):
        self.__save()
        self.__bio.seek(0)
        return self.__bio.read()

    @property
    def format_percent(self):
       return self.__format_percent

    @property
    def format_number(self):
       return self.__format_number

    def to_file(self, file):
        # if file is just a pain string you have to create the file and open it for writing
        if isinstance(file, str):
            with open(file, mode="w+b") as f:
                f.write(self.stream)

        else:
            # this may rise an error
            file.write(self.stream)


def excelBook(title, subject):
    e = Excel(date_format='yyyy/mm/dd')
    e.book.set_properties({
        'title': title,
        'subject': subject,
        'company': 'Lobnek Wealth Management',
    })

    return e
