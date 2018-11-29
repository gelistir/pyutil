import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.custodian import Currency
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


#def rrr(name, *kwargs):
#    x = relationship(Series, primaryjoin=ProductInterface.join_series(name), **kwargs)
#    association_proxy("_{x}".format(x=name), "data")

class Security(ProductInterface):
    @staticmethod
    def create_volatility(currency, data):
        assert isinstance(currency, Currency)
        assert isinstance(data, pd.Series)

        return Series(name="volatility", product2=currency, data=data)

    __tablename__ = "security"
    __mapper_args__ = {"polymorphic_identity": "Security"}
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)

    # define the price...
    _price = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"))

    price = association_proxy("_price", "data", creator=lambda data: Series(name="price", data=data))

    # define the volatility (dictionary where currency is the key!)
    _vola = relationship(Series, collection_class=attribute_mapped_collection("product_2"),
                          primaryjoin=ProductInterface.join_series("volatility"))

    vola = association_proxy("_vola", "data",
                             creator=lambda currency, data: Security.create_volatility(currency=currency, data=data))


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
