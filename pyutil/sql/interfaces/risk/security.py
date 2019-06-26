import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property


from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.ref import Field, FieldType, DataType
from pyutil.timeseries.merge import last_index

Field_BMulti = Field(name="Bloomberg Multiplier", type=FieldType.dynamic, result=DataType.float)
Field_KIID = Field(name="KIID", type=FieldType.dynamic, result=DataType.integer)
Field_Ticker = Field(name="Bloomberg Ticker", type=FieldType.dynamic, result=DataType.string)
Field_Name = Field(name="Name", type=FieldType.dynamic, result=DataType.string)


class Security(ProductInterface):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(name, **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.reference.get(Field_Name))

    @hybrid_property
    def kiid(self):
        return self.reference.get(Field_KIID)

    @hybrid_property
    def bloomberg_ticker(self):
        return self.reference.get(Field_Ticker, None)

    @hybrid_property
    def bloomberg_scaling(self):
        return self.reference.get(Field_BMulti, 1.0)

    @property
    def price(self):
        return self.read(kind="PRICE")

    @price.setter
    def price(self, data):
        self.write(data=data, kind="PRICE")

    @staticmethod
    def prices(securities):
        return pd.DataFrame({security.name: security.price for security in securities})

    def upsert_price(self, data):
        self.merge(data, kind="PRICE")

    @property
    def last(self):
        return last_index(self.price)



    # def to_json(self):
    #    nav = fromNav(self._price)
    #    return {"Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown, "name": self.name}

    # @property
    # def vola_frame(self):
    #    return pd.DataFrame({key: item for (key, item) in self._vola.items()})

    #@staticmethod
    #def frame(securities):
    #    frame = pd.DataFrame(
    #        {security: {**security.reference_series, **{"Name": security.fullname, "Entity ID": int(security.name)}} for
    #         security in securities}).transpose()
    #    frame.index.name = "Security"
    #    return frame.sort_index()

    #@staticmethod
    #def read_prices(**kwargs):
    #    return Security.__collection__.frame(key="name", **kwargs)

    #@staticmethod
    #def frame(**kwargs):
    #    return Security.__collection__.frame(key="name", **kwargs)