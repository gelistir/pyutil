import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.series import Series
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

    def __init__(self, name, kiid=None, ticker=None):
        super().__init__(name)
        if kiid:
            self.reference[FIELDS["Lobnek KIID"]] = kiid

        if ticker:
            self.reference[FIELDS["Lobnek Ticker Symbol Bloomberg"]] = ticker


    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.get_reference("Name"))

    @hybrid_property
    def kiid(self):
        return self.get_reference("KIID")

    @hybrid_property
    def bloomberg_ticker(self):
        return self.get_reference("Bloomberg Ticker", default=None)

    @hybrid_property
    def bloomberg_scaling(self):
        return self.get_reference("Bloomberg Multiplier", default=1.0)

    def to_json(self):
        nav = fromNav(self.price)
        return {"Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown, "name": self.name}

    @property
    def vola_frame(self):
        return pd.DataFrame({key: item for (key, item) in self.vola.items()})


class Price(Series):
    __tablename__ = "price"
    __mapper_args__ = {"polymorphic_identity": "price"}
    id = sq.Column(sq.ForeignKey('series.id'), primary_key=True)

    __security_id = sq.Column("security_id", sq.Integer, sq.ForeignKey(Security.id), nullable=False)

    def __init__(self, data=None):
        self.data = data


Security._price = relationship(Price, uselist=False, backref="security")
Security.price = association_proxy("_price", "data", creator=lambda data: Price(data=data))


class VolatilitySecurity(Series):
    __tablename__ = "volatility_security"
    __mapper_args__ = {"polymorphic_identity": "volatility_security"}
    id = sq.Column(sq.ForeignKey('series.id'), primary_key=True)

    __security_id = sq.Column("security_id", sq.Integer, sq.ForeignKey(Security.id), nullable=False)
    __currency_id = sq.Column("currency_id", sq.Integer, sq.ForeignKey(Currency.id), nullable=False)
    __currency = relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    @hybrid_property
    def currency(self):
        return self.__currency

    def __init__(self, currency=None, data=None):
        self.__currency = currency
        self.data = data


Security._vola = relationship(VolatilitySecurity, collection_class=attribute_mapped_collection("currency"), cascade="all, delete-orphan", backref="security")
Security.vola = association_proxy('_vola', 'data', creator=lambda currency, data: VolatilitySecurity(currency=currency, data=data))
