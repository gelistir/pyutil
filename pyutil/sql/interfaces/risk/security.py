from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.products import ProductInterface, Products
from pyutil.sql.model.ref import Field, DataType, FieldType


FIELDS = {
    "Lobnek Ticker Symbol Bloomberg": Field(name="Bloomberg Ticker", result=DataType.string, type=FieldType.other),
    "Lobnek Geographic Focus": Field(name="Geography", result=DataType.string, type=FieldType.other),
    "Lobnek KIID": Field(name="KIID", result=DataType.integer, type=FieldType.other),
    "Lobnek Liquidity": Field(name="Liquidity", result=DataType.string, type=FieldType.other),
    "Lobnek Price Multiplier Bloomberg": Field(name="Bloomberg Multiplier", result=DataType.float, type=FieldType.other),
    "Lobnek Sub Asset Class": Field(name="Sub Asset Class", result=DataType.string, type=FieldType.other),
    "Lobnek Reporting Asset Class": Field(name="Asset Class", result=DataType.string, type=FieldType.other),
    "Currency": Field(name="Currency", result=DataType.string, type=FieldType.other),
    "Lobnek Risk Monitoring Security Name": Field(name="Risk Security Name", result=DataType.string, type=FieldType.other),
    "name": Field(name="Name", result=DataType.string, type=FieldType.other)
}


class Security(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Security"}

    @property
    def price(self):
        return fromNav(self.get_timeseries(name="price"), adjust=False)

    def price_upsert(self, ts):
        self.upsert_ts(name="price", data=ts)

    def volatility_upsert(self, ts, currency):
        self.upsert_ts(name="volatility", data=ts, secondary=currency)

    @property
    def volatility(self):
        return self.frame("volatility")

    @hybrid_property
    def kiid(self):
        return self.get_reference(field=FIELDS["Lobnek KIID"])

    @property
    def bloomberg_ticker(self):
        return self.get_reference(field=FIELDS["Lobnek Ticker Symbol Bloomberg"])

    def to_html_dict(self):
        return fromNav(ts=self.price, adjust=False).to_dictionary(name=self.name)


class Securities(Products):
    def __init__(self, securities):
        super().__init__(securities, cls=Security, attribute="name")

