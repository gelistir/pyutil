
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
from pyutil.timeseries.merge import merge

FIELDS = {
    #"name": Field(name="Name", result=DataType.string, type=FieldType.other),
    #"15. Custodian Name": Field(name="Custodian", result=DataType.string, type=FieldType.other),
    #"17. Reference Currency": Field(name="Currency", result=DataType.string, type=FieldType.other),
    #"18. LWM Risk Profile": Field(name="Risk Profile", result=DataType.string, type=FieldType.other),
    #"23. LWM - AUM Type": Field(name="AUM Type", result=DataType.string, type=FieldType.other),
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
    fullname = sq.Column("fullname", sq.String, nullable=True)
    #risk_profile = sq.Column("risk_profile", sq.String, nullable=True)
    #aum_type = sq.Column("aum_type", sq.String, nullable=True)

    __currency_id = sq.Column("currency_id", sq.Integer, sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    # returns
    _returns_rel = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("returns"), cascade="all, delete-orphan")
    _returns = association_proxy("_returns_rel", "data", creator=lambda data: Series(name="returns", data=data))

    # volatility
    _volatility_rel = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("volatility"), cascade="all, delete-orphan")
    _volatility = association_proxy("_volatility_rel", "data", creator=lambda data: Series(name="volatility", data=data))

    # position
    _position_rel = relationship(Series, collection_class=attribute_mapped_collection("key"), cascade="all, delete-orphan",
                             primaryjoin=ProductInterface.join_series("position"))

    _position = association_proxy("_position_rel", "data", creator=lambda s, data: Owner.create_position(security=s[0], custodian=s[1], data=data))

    def __init__(self, name, currency=None):
        super().__init__(name=name)
        self.currency = currency

    def __repr__(self):
        return "Owner({id}: {name})".format(id=self.name, name=self.get_reference("Name"))

    @hybrid_property
    def currency(self):
        return self.__currency

    @currency.setter
    def currency(self, value):
        self.__currency = value

    @property
    def securities(self):
        return set([x[0] for x in self._position.keys()])

    @property
    def custodians(self):
        return set([x[1] for x in self._position.keys()])

    @property
    def position_frame(self):
        a = pd.DataFrame(dict(self._position)).transpose().stack()
        if not a.empty:
            a.index.names = ["Security", "Custodian", "Date"]

        return a.to_frame(name="Position")

    @property
    def vola_security_frame(self):
        x = pd.DataFrame({security: security.volatility(self.currency) for security in set(self.securities)}).stack()
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

        try:
            position_reference = position.join(reference, on="Security")
            return position_reference.join(volatility, on=["Security", "Date"])
        except KeyError:
            return pd.DataFrame({})

    def to_json(self):
        ts = fromReturns(r=self._returns)
        return {"name": self.name, "Nav": ts, "Volatility": ts.ewm_volatility(), "Drawdown": ts.drawdown}

    def upsert_position(self, security, custodian, ts):
        assert isinstance(security, Security)
        assert isinstance(custodian, Custodian)

        key = (security, custodian)
        self._position[key] = merge(new=ts, old=self._position.get(key, default=None))
        return self._position[key]

    def upsert_volatility(self, ts):
        self._volatility = merge(new=ts, old=self._volatility)
        return self.volatility

    def upsert_returns(self, ts):
        self._returns = merge(new=ts, old=self._returns)
        return self.returns

    @property
    def volatility(self):
        return self._volatility

    @property
    def returns(self):
        return self._returns

    def flush(self):
        # delete all positions...
        self._position.clear()
