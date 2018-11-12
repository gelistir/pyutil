import pandas as pd
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface, Timeseries
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.performance.summary import fromNav


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

    def upsert_volatility(self, currency, ts):
        name = "volatility_{currency}".format(currency=currency.name)
        self.ts[name] = Timeseries.merge(new=ts, old=self.get_ts(name))
        return self.volatility(currency=currency)

    def volatility(self, currency):
        assert isinstance(currency, Currency)
        return self.get_ts(field="volatility_{currency}".format(currency=currency.name), default=pd.Series({}))

    def to_json(self):
        nav = fromNav(self.get_ts("price", default=pd.Series({})))
        return {"Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown, "name": self.name}
