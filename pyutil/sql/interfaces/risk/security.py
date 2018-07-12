import pandas as pd

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

    @property
    def bloomberg_ticker(self):
        return self.get_reference("Bloomberg Ticker")

    def upsert_volatility(self, client, currency, ts):
        assert isinstance(currency, Currency)
        client.write_series(ts=ts, tags={"security": self.name, "currency": currency.name}, field="volatility", measurement="VolatilitySecurity")

    def upsert_price(self, client, ts):
        client.write_series(ts=ts, tags={"security": self.name}, field="price", measurement="PriceSecurity")

    def price(self, client):
        return client.read_series(field="price", measurement="PriceSecurity", conditions={"security": self.name})

    def volatility(self, client, currency):
        assert isinstance(currency, Currency)
        return client.read_series(field="volatility", measurement="VolatilitySecurity", conditions={"security": self.name, "currency": currency.name})

    @staticmethod
    def prices_all(client):
        return client.read_series(measurement="PriceSecurity", field="price", tags=["security"], unstack=True)

    @staticmethod
    def volatility_all(client):
        return client.read_series(measurement="VolatilitySecurity", field="volatility", tags=["security", "currency"])

    @staticmethod
    def reference_frame(securities):
        d = dict()

        for security in securities:
            x = {field.name: field.result.parse(value) for field, value in security.reference.items()}

            if x:
                d[security.name] = pd.Series(x)

        return pd.DataFrame(d).transpose().fillna("")


        #return f[["content", "result"]].apply(lambda x: x[1].parse(x[0]), axis=1).unstack(level=-1)