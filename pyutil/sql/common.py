import enum as enum
import pandas as pd


class FieldType(enum.Enum):
    dynamic = "dynamic"
    static = "static"
    other = "other"


class DataType(enum.Enum):
    string = ("string", lambda x: x)
    integer = ("integer", lambda x: int(x))
    float = ("float", lambda x: float(x))
    date = ("date", lambda x: pd.to_datetime(int(x)*1e6).date())
    datetime = ("datetime", lambda x: pd.to_datetime(int(x)*1e6))
    percentage = ("percentage", lambda x: float(x))

    def __init__(self, value, fct):
        self.__v = value
        self.__fct = fct

    @property
    def value(self):
        # this is a bit of a hack, we are overriding the value attribute of the enum...
        return self.__v

    def __call__(self, *args):
        return self.__fct(*args)
