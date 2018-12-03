
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship, relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.performance.summary import fromReturns
from pyutil.sql.interfaces.products import ProductInterface

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.interfaces.series import Series
from pyutil.sql.model.ref import Field, DataType, FieldType

FIELDS = {
    "name": Field(name="Name", result=DataType.string, type=FieldType.other),
    "15. Custodian Name": Field(name="Custodian", result=DataType.string, type=FieldType.other),
    "17. Reference Currency": Field(name="Currency", result=DataType.string, type=FieldType.other),
    "18. LWM Risk Profile": Field(name="Risk Profile", result=DataType.string, type=FieldType.other),
    "23. LWM - AUM Type": Field(name="AUM Type", result=DataType.string, type=FieldType.other),
    "Inception Date": Field(name="Inception Date", result=DataType.string, type=FieldType.other)
    # don't use date here...
}


class Owner(ProductInterface):
    @staticmethod
    def create_position(security, custodian, data):
        assert isinstance(security, Security)
        assert isinstance(custodian, Custodian)
        assert isinstance(data, pd.Series)

        return Series(name="position", product2=security, product3=custodian, data=data)

    __tablename__ = "owner"
    __mapper_args__ = {"polymorphic_identity": "Owner"}
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)
    active = sq.Column("active", sq.Boolean)

    #__securities = _relationship(Security, secondary=_association_table, backref="owner", lazy="joined")
    __currency_id = sq.Column("currency_id", sq.Integer, sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")
    __custodian_id = sq.Column("custodian_id", sq.Integer, sq.ForeignKey(Custodian.id), nullable=True)
    __custodian = _relationship(Custodian, foreign_keys=[__custodian_id], lazy="joined")


    # returns
    _returns = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("returns"), cascade="all, delete-orphan")
    returns = association_proxy("_returns", "data", creator=lambda data: Series(name="returns", data=data))

    # volatility
    _volatility = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("volatility"), cascade="all, delete-orphan")
    volatility = association_proxy("_volatility", "data", creator=lambda data: Series(name="volatility", data=data))

    # position
    _position = relationship(Series, collection_class=attribute_mapped_collection("key"), cascade="all, delete-orphan",
                             primaryjoin=ProductInterface.join_series("position"))

    position = association_proxy("_position", "data", creator=lambda s, data: Owner.create_position(security=s[0], custodian=s[1], data=data))




    def __init__(self, name, currency=None, custodian=None):
        super().__init__(name=name)
        self.currency = currency
        self.custodian = custodian

    def __repr__(self):
        return "Owner({id}: {name})".format(id=self.name, name=self.get_reference("Name"))

    @hybrid_property
    def currency(self):
        return self.__currency

    @currency.setter
    def currency(self, value):
        self.__currency = value

    @hybrid_property
    def custodian(self):
        return self.__custodian

    @custodian.setter
    def custodian(self, value):
        self.__custodian = value

    @property
    def securities(self):
        return set([x.security for x in self.position])


    @property
    def position_frame(self):
        d = dict()

        for tags, data in self.position.items():
            d[tags] = data

        a = pd.DataFrame(d).transpose().stack()
        a.index.names = ["Security", "Custodian", "Date"]
        return a.to_frame(name="Position")


    @property
    def vola_security_frame(self):
        x = pd.DataFrame({security.name: security.vola.get(self.currency, pd.Series({})) for security in set(self.securities)}).stack()
        x.index.names = ["Date", "Security"]
        return x.swaplevel().to_frame("Volatility")


    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities, name="Security")


    @property
    def position_reference(self):
        reference = self.reference_securities
        position = self.position_frame
        volatility = self.vola_security_frame

        position_reference = position.join(reference, on="Security")

        return position_reference.join(volatility, on=["Security", "Date"])

    def to_json(self):
        r = self.returns
        ts = fromReturns(r=r)
        return {"name": self.name, "Nav": ts, "Volatility": ts.ewm_volatility(), "Drawdown": ts.drawdown}

