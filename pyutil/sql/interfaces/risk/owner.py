
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
from pyutil.timeseries.merge import merge, to_datetime, to_date

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



    def __init__(self, name, currency=None):
        super().__init__(name=name)
        self.currency = currency
        #self.custodian = custodian

    def __repr__(self):
        return "Owner({id}: {name})".format(id=self.name, name=self.get_reference("Name"))

    @hybrid_property
    def currency(self):
        return self.__currency

    @currency.setter
    def currency(self, value):
        self.__currency = value

    #@hybrid_property
    #def custodian(self):
    #    return self.__custodian

    #@custodian.setter
    #def custodian(self, value):
    #    self.__custodian = value

    @property
    def securities(self):
        return set([x[0] for x in self.position.keys()])

    @property
    def custodians(self):
        return set([x[1] for x in self.position.keys()])


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
        x = pd.DataFrame({security: security.vola.get(self.currency, pd.Series({})) for security in set(self.securities)}).stack()
        x.index.names = ["Date", "Security"]
        return x.swaplevel().to_frame("Volatility")


    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities, name="Security", objectnotation=False).sort_index(axis=0)


    @property
    def position_reference(self):
        reference = Security.reference_frame(self.securities, name="Security", objectnotation=True).sort_index(axis=0)
        position = self.position_frame
        volatility = self.vola_security_frame

        position_reference = position.join(reference, on="Security")

        return position_reference.join(volatility, on=["Security", "Date"])

    def to_json(self):
        ts = fromReturns(r=self.returns)
        return {"name": self.name, "Nav": ts, "Volatility": ts.ewm_volatility(), "Drawdown": ts.drawdown}

    def upsert_position(self, security, custodian, ts):
        assert isinstance(security, Security)
        assert isinstance(custodian, Custodian)

        key = (security, custodian)
        self.position[key] = merge(new=ts, old=self.position.get(key, default=None))
        return self.position[key]

    def upsert_volatility(self, ts):
        self.volatility = merge(new=ts, old=self.volatility)
        return self.volatility

    def upsert_returns(self, ts):
        self.returns = merge(new=to_date(ts), old=self.returns)
        return self.returns

