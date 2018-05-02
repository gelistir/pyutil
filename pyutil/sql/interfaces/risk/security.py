import sqlalchemy as _sq
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
    __entity_id = _sq.Column("entity_id", _sq.Integer, unique=True, nullable=False)

    def __init__(self, entity_id):
        self.__entity_id = entity_id

    def __repr__(self):
        return "Security({entity})".format(entity=self.entity_id,)

    @hybrid_property
    def entity_id(self):
        return self.__entity_id

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


class Securities(object):
    def __init__(self, securities):
        for a in securities:
            assert isinstance(a, Security)
        self.__securities = {s.name: s for s in securities}

    @property
    def reference(self):
        return Products(self.__securities.values()).reference

    def __getitem__(self, item):
        return self.__securities[item]

    def __iter__(self):
        for security in self.__securities.values():
            yield security
