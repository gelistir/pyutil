from io import BytesIO

import pandas as pd
from pandas.io.formats.style import Styler


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

    #def add_format(self, format):
    #    return self.__writer.book.add_format(format)

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


class Excel2(object):
    """
    Interaction with Excel
    """
    def __init__(self, date_format='dd/mm', **kwargs):
        self.__bio = BytesIO()
        self.__writer = pd.ExcelWriter(self.__bio, engine='openpyxl', date_format=date_format, **kwargs)

    def add_frame(self, frame, sheetname):
        frame.to_excel(self.__writer, sheet_name=sheetname, engine="openpyxl")
        return self.__writer.sheets[sheetname]

    def __save(self):
        self.__writer.save()
        return self

    @property
    def stream(self):
        self.__save()
        self.__bio.seek(0)
        return self.__bio.read()


    def to_file(self, file):
        # if file is just a pain string you have to create the file and open it for writing
        if isinstance(file, str):
            with open(file, mode="w+b") as f:
                f.write(self.stream)

        else:
            # this may rise an error
            file.write(self.stream)


if __name__ == '__main__':
    fff = pd.DataFrame(data=[3.0, 4.0241552,-2.01255])

    def mod(frame):
        print("Hello2")
        return frame.style.highlight_max(color="blue").highlight_min().set_precision(2)


    #fff.style.highlight_max(color="blue").highlight_min().format("{:+.2f}").to_excel('styled.xlsx', engine='openpyxl')
    mod(fff).to_excel('styled5.xlsx', engine='openpyxl')

    ee = Excel2()
    ee.add_frame(mod(fff), sheetname="wurst")
    ee.to_file("bbb2xlsx")


    EasyStyler = Styler.from_custom_template("templates", "myhtml.tpl")
    print(EasyStyler(x).render())

    #from matplotlib.pyplot import get_cmap

    #cm = get_cmap(name="greens")

    #s = fff.style.background_gradient(cmap=cm)#.to_excel('styled4.xlsx', engine='openpyxl')
