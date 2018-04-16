import enum as enum
import pandas as pd


# todo: test this
def reference(products):
    x = pd.DataFrame({product: product.reference.to_pandas() for product in products}).transpose()
    x.index.name = "Product"
    return x


def history(products, field="PX_LAST"):
    # note you can use this
    x = pd.DataFrame({product: product.timeseries[field] for product in products})
    # or this
    # x = pd.DataFrame({product: product.timeseries[field] for product in products})
    # the second option will crash as there is no timeseries field for the product

    x.index.name = "Date"
    # delete empty columns
    return x.dropna(axis=1, how="all")


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
