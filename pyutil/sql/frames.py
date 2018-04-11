from io import BytesIO

import pandas as pd
from sqlalchemy import String, LargeBinary, Column

from pyutil.sql.base import Base


class Frame(Base):
    __tablename__ = 'frame'
    name = Column(String, primary_key=True)
    _data = Column("data", LargeBinary)
    _index = Column("index", String)

    @property
    def frame(self):
        json_str = BytesIO(self._data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self._index.split(","))

    @frame.setter
    def frame(self, value):
        self._index = ",".join(value.index.names)
        self._data = value.reset_index().to_json(orient="split", date_format="iso").encode()