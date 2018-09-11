from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
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

from collections import namedtuple

Price = namedtuple('Price', ['date', 'security', 'value'])

Volatility = namedtuple('Volatility', ['date', 'security', 'currency', 'value'])


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

    def upsert_volatility(self, currency, ts):
        assert isinstance(currency, Currency)
        self._ts_upsert(ts=ts, tags={"currency": currency.name}, field="volatility", measurement="VolatilitySecurity")

    def upsert_price(self, ts):
        self._ts_upsert(ts=ts, field="price", measurement="PriceSecurity")

    @property
    def price(self):
        for date, value in self._ts(field="price", measurement="PriceSecurity").items():
            yield Price(date=date, value=value, security=self)

    def volatility(self, currency):
        assert isinstance(currency, Currency)
        for date, value in self._ts(field="volatility", measurement="VolatilitySecurity", conditions={"currency": currency.name}).items():
            yield Volatility(date=date, value=value, security=self, currency=currency)

