from sqlalchemy.ext.hybrid import hybrid_property
from pyutil.sql.interfaces.products import ProductInterface
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

    def price(self, client):
        return client.series(field="price", measurement="security", conditions=[("security", self.name)], date=True)

    def volatility(self, client, currency):
        return client.series(field="volatility", measurement="security", conditions=[("security", self.name), ("currency", currency)], date=True)

    @hybrid_property
    def kiid(self):
        return self.get_reference("KIID")

    @property
    def bloomberg_ticker(self):
        return self.get_reference("Bloomberg Ticker")

    def upsert_volatility(self, client, currency, ts):
        if len(ts) > 0:
            helper = client.helper(tags=["security", "currency"], fields=["volatility"], series_name='security', autocommit=True, bulk_size=10)

            for date, value in ts.items():
                helper(security=self.name, volatility=float(value), currency=currency, time=date)

            helper.commit()

    def upsert_price(self, client, ts):
        if len(ts) > 0:
            helper = client.helper(tags=["security"], fields=["price"], series_name='security', autocommit=True, bulk_size=10)

            for date, value in ts.items():
                helper(security=self.name, price=float(value), time=date)

            helper.commit()
