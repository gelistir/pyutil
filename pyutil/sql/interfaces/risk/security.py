import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.ref import Field, FieldType, DataType

#def _create_volatility(currency, data):
#    assert isinstance(currency, Currency)
#    assert isinstance(data, pd.Series)

#    return Series(name="volatility", product2=currency, data=data)


Field_BMulti = Field(name="Bloomberg Multiplier", type=FieldType.dynamic, result=DataType.float)
Field_KIID = Field(name="KIID", type=FieldType.dynamic, result=DataType.integer)
Field_Ticker = Field(name="Bloomberg Ticker", type=FieldType.dynamic, result=DataType.string)
Field_Name = Field(name="Name", type=FieldType.dynamic, result=DataType.string)


class Security(ProductInterface):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    # define the price...
    #_price_rel = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"), lazy="joined")

    #_price = association_proxy("_price_rel", "data", creator=lambda data: Series(name="price", data=data))

    # define the volatility (dictionary where currency is the key!)
    #_vola_rel = relationship(Series, collection_class=attribute_mapped_collection("product_2"),
    #                     primaryjoin=ProductInterface.join_series("volatility"), lazy="joined")

    #_vola = association_proxy(target_collection="_vola_rel", attr="data",
    #                          creator=lambda currency, data: _create_volatility(currency=currency, data=data))

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(name, **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.reference.get(Field_Name))

    @hybrid_property
    def kiid(self):
        return self.reference.get(Field_KIID)
        #return self.get_reference("KIID")

    @hybrid_property
    def bloomberg_ticker(self):
        return self.reference.get(Field_Ticker, None)

        #return self.get_reference("Bloomberg Ticker", default=None)

    @hybrid_property
    def bloomberg_scaling(self):
        return self.reference.get(Field_BMulti, 1.0)

    #def to_json(self):
    #    nav = fromNav(self._price)
    #    return {"Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown, "name": self.name}

    #@property
    #def vola_frame(self):
    #    return pd.DataFrame({key: item for (key, item) in self._vola.items()})

    #def upsert_volatility(self, currency, ts):
    #    assert isinstance(currency, Currency)
    #    self._vola[currency] = merge(new=ts, old=self._vola.get(currency, default=None))
    #    return self._vola[currency]

    #def upsert_price(self, ts):
    #    # self.price will be None if not defined
    #    self._price = merge(new=ts, old=self._price)
    #    return self.price

    #@property
    #def price(self):
    #    return self._price

    #def volatility(self, currency):
    #    return self._vola.get(currency, pd.Series({}))

    @staticmethod
    def frame(securities):
        frame = pd.DataFrame({security: {**security.reference_series, **{"Name": security.fullname, "Entity ID": int(security.name)}} for security in securities}).transpose()
        frame.index.name = "Security"
        return frame.sort_index()
