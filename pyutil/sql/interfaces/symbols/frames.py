from io import BytesIO

import pandas as pd
from sqlalchemy import LargeBinary, Column
from pyutil.sql.interfaces.products import ProductInterface


class Frame(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "frame"}
    __data = Column("data", LargeBinary)

    def __init__(self, name, frame=None):
        super().__init__(name)
        if frame is not None:
            # this calls the setter further below...
            self.frame = frame

    @property
    def frame(self):
        json_str = BytesIO(self.__data).read().decode()
        return pd.read_json(json_str, orient="table")

    @frame.setter
    def frame(self, value):
        assert isinstance(value, pd.DataFrame), "Value has to be a DataFrame. It is currently {t}".format(t=type(value))
        self.__data = value.to_json(orient="table", date_format="iso").encode()