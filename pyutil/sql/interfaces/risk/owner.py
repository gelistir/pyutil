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
from pyutil.timeseries.merge import merge


def _create_position(security, custodian, data):
    assert isinstance(security, Security)
    assert isinstance(custodian, Custodian)
    assert isinstance(data, pd.Series)

    return Series(name="position", product2=security, product3=custodian, data=data)


class Owner(ProductInterface):
    __tablename__ = "owner"
    __mapper_args__ = {"polymorphic_identity": "Owner"}
    id = sq.Column(sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    fullname = sq.Column("fullname", sq.String, nullable=True)

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

    _position = association_proxy("_position_rel", "data", creator=lambda s, data: _create_position(security=s[0], custodian=s[1], data=data))

    def __init__(self, name, currency=None, fullname=None):
        super().__init__(name=name)
        self.currency = currency
        self.fullname = fullname

    def __repr__(self):
        return "Owner({id}: {fullname}, {currency})".format(id=self.name, fullname=self.fullname, currency=self.currency.name)

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
        a = pd.DataFrame(dict(self._position))
        if not a.empty:
            a = a.transpose().stack()

            a.index.names = ["Security", "Custodian", "Date"]
            return a.to_frame(name="Position")
        else:
            return a

    @property
    def vola_security_frame(self):
        assert self.currency, "The currency for the owner is not specified!"
        x = pd.DataFrame({security: security.volatility(self.currency) for security in set(self.securities)})\

        if not x.empty:
            x = x.stack()
            x.index.names = ["Date", "Security"]
            return x.swaplevel().to_frame("Volatility")

        return x

    @property
    def reference_securities(self):
        return Security.frame(self.securities)

    @property
    def position_reference(self):
        reference = self.reference_securities
        position = self.position_frame
        volatility = self.vola_security_frame

        #print(reference)
        #print(position)
        #print(volatility)

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
        return self.position(security=security, custodian=custodian)

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

    def position(self, security, custodian):
        return self._position[(security, custodian)]

    def flush(self):
        # delete all positions...
        self._position.clear()

    @staticmethod
    def frame(owners):
        frame = pd.DataFrame({owner: {**owner.reference_series, **{"Entity ID": int(owner.name), "Name": owner.fullname, "Currency": owner.currency.name}} for owner in owners}).transpose()
        frame.index.name = "Security"
        return frame.sort_index()