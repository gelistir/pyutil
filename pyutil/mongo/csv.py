from .abc_archive import Archive
import pandas as pd

class CsvArchive(Archive):
    def __init__(self):
        self.__data = dict()

    def put(self, frame, name="PX_LAST"):
        self.__data[name] = frame

    def history(self, items, name="PX_LAST", before=pd.Timestamp("2002-01-01")):
        return self.__data[name][items].truncate(before=before)