from io import BytesIO

import pandas as pd
from sqlalchemy import String, LargeBinary, Column
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface


class Frame(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "frame"}
    __name = Column("name", String, unique=True)
    __data = Column("data", LargeBinary)
    __index = Column("index", String)

    def __init__(self, name, frame=None):
        self.__name = name
        if frame is not None:
            self.frame = frame

    @hybrid_property
    def name(self):
        return self.__name

    @property
    def frame(self):
        json_str = BytesIO(self.__data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self.__index.split(","))

    @frame.setter
    def frame(self, value):
        self.__index = ",".join(value.index.names)
        self.__data = value.reset_index().to_json(orient="split", date_format="iso").encode()