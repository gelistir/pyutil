import pandas as pd
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

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.get_reference(field=FIELDS["name"]))

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
        return fromNav(ts=self.price, adjust=False).to_dictionary(name=self.get_reference("Name"))


class Securities(Products):
    def __init__(self, securities):
        super().__init__(securities, cls=Security, attribute="name", f=lambda x: int(x))

    def __repr__(self):
        d = {key: product for key, product in self.to_dict().items()}
        seq = ["{key:10d}   {product}".format(key=key, product=d[key]) for key in sorted(d)]
        return "\n".join(seq)

    def to_html_dict(self, index_name="Entity ID"):
        return self.to_html(index_name=index_name)

    @property
    def prices(self):
        f = pd.DataFrame({sec.name : sec.get_timeseries("price") for sec in self}).transpose()
        f = f.stack(level=0).dropna().to_frame(name="Price").sort_index(level=1, ascending=False)
        f.index.names=["Owned ID","Date"]
        return f

    @property
    def securities_volatility(self):
        frame = pd.concat({sec.name: sec.frame("volatility", rename=True).stack() for sec in self}, axis=0)
        frame = frame.to_frame(name="Volatility")
        frame.index.names = ["Security", "Date", "Currency"]
        return frame