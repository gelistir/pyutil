import pandas as pd
import sqlalchemy as _sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.products import ProductInterface, association_table
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.model.ref import Field, DataType, FieldType

_association_table = association_table(left="security", right="owner", name="security_owner")

FIELDS = {
    "name": Field(name="Name", result=DataType.string, type=FieldType.other),
    "15. Custodian Name": Field(name="Custodian", result=DataType.string, type=FieldType.other),
    "17. Reference Currency": Field(name="Currency", result=DataType.string, type=FieldType.other),
    "18. LWM Risk Profile": Field(name="Risk Profile", result=DataType.string, type=FieldType.other),
    "23. LWM - AUM Type": Field(name="AUM Type", result=DataType.string, type=FieldType.other),
    "Inception Date": Field(name="Inception Date", result=DataType.string, type=FieldType.other)
    # don't use date here...
}

from collections import namedtuple

Position = namedtuple('Position', ['date', 'security', 'custodian', 'value', 'owner'])
Volatility = namedtuple('Volatility', ['date', 'owner', 'security', 'currency', 'value'])


class Owner(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Owner"}
    __securities = _relationship(Security, secondary=_association_table, backref="owner", lazy="joined")
    __currency_id = _sq.Column("currency_id", _sq.Integer, _sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")
    __custodian_id = _sq.Column("custodian_id", _sq.Integer, _sq.ForeignKey(Custodian.id), nullable=True)
    __custodian = _relationship(Custodian, foreign_keys=[__custodian_id], lazy="joined")

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
        return self.__securities

    @property
    def __position(self):
        for security in self.securities:
            for (time, custodian), value in self._ts(field="weight", measurement="WeightsOwner", tags=["custodian"],
                                                     conditions={"security": security.name}).items():
                yield Position(date=time, custodian=custodian, security=security, value=value, owner=self)

    def position(self, index_col=None):
        if not index_col:
            yield from self.__position
        else:
            for position in self.__position:
                yield Position(date=position.date, custodian=position.custodian, security=position.security,
                               value=position.value * position.security.get_reference(index_col), owner=position.owner)

    @property
    def __volatility(self):
        for security in self.securities:
            for vola in security.volatility(currency=self.currency):
                yield Volatility(date=vola.date, currency=self.currency, security=vola.security, value=vola.value,
                                 owner=self)

    def vola(self, index_col=None):
        if not index_col:
            yield from self.__volatility
        else:
            for vola in self.__volatility:
                yield Volatility(date=vola.date, security=vola.security, currency=self.currency,
                                 value=vola.value * vola.security.get_reference(index_col), owner=self)

    @property
    def __vola_weighted(self):
        v = pd.DataFrame([v for v in self.vola()]).set_index(keys=["owner", "security", "date"])["value"]
        w = pd.DataFrame([w for w in self.position()]).set_index(keys=["owner", "security", "date"])["value"]

        for (owner, security, date), value in w.multiply(v).items():
            yield Volatility(owner=owner, security=security, date=date, currency=self.currency, value=value)

    def vola_weighted(self, index_col=None):
        # volatility * weight
        if not index_col:
            yield from self.__vola_weighted
        else:
            for volatility in self.__vola_weighted:
                yield Volatility(owner=self, security=volatility.security, date=volatility.date, currency=self.currency, value=volatility.security.get_reference(index_col) * volatility.value)


    @property
    def reference_securities(self):
        return pd.DataFrame({security.name: security.reference_series.sort_index() for security in
                             self.securities}).sort_index().transpose()

    @property
    def kiid(self):
        for security in self.securities:  # {security.name: security.kiid for security in self.securities})
            yield security.name, security.kiid

    # def kiid_weighted(self, sum=False, tail=None):
    #    x = self.position(sum=False, tail=tail, custodian=False).transpose().apply(lambda weights: weights * self.kiid, axis=0).dropna(axis=0, how="all")
    #    if sum:
    #        x.loc["Sum"] = x.sum(axis=0)
    #    return x

    # def kiid_weighted_by(self, index_col, sum=False, tail=None):#
    #
    #        kiid = self.kiid_weighted(sum=False, tail=tail)
    #        return self.__weighted_by(x=kiid, index_col=index_col, sum=sum)

    def upsert_return(self, ts):
        Owner.client.write_series(ts=ts, field="return", tags={"owner": self.name}, measurement='ReturnOwner')
        try:
            x = self.returns
            x = pd.concat((pd.Series({(x.index[0] - pd.DateOffset(days=1)).date(): 0.0}), x))
            return Owner.client.write_series(field="nav", ts=(x + 1.0).cumprod(), tags={"owner": self.name},
                                             measurement='ReturnOwner')
        except:
            return Owner.client.write_series(field="nav", ts=pd.Series({}), tags={"owner": self.name},
                                             measurement='ReturnOwner')

    def upsert_position(self, security, ts, custodian=None):
        assert isinstance(security, Security)

        # if not custodian:
        custodian = custodian or self.custodian

        assert isinstance(custodian, Custodian)

        Owner.client.write_series(ts=ts, field="weight",
                                  tags={"owner": self.name, "security": security.name, "custodian": custodian.name},
                                  measurement="WeightsOwner")

    def upsert_volatility(self, ts):
        Owner.client.write_series(ts=ts, field="volatility", tags={"owner": self.name}, measurement='VolatilityOwner')

    @property
    def returns(self):
        # this is fast!
        return Owner.client.read_series(field="return", measurement="ReturnOwner", conditions={"owner": self.name})

    @property
    def volatility(self):
        return Owner.client.read_series(field="volatility", measurement="VolatilityOwner",
                                        conditions={"owner": self.name})

    @property
    def nav(self):
        return fromNav(
            Owner.client.read_series(field="nav", measurement='ReturnOwner', conditions={"owner": self.name}))

    @property
    def reference_securities(self):
        for s in self.securities:
            yield s, s.reference
    #
    # @staticmethod
    # def returns_all():
    #     warnings.warn("deprecated", DeprecationWarning)
    #     return Owner.client.read_frame(measurement="ReturnOwner", field="return", tags=["owner"])
    #
    # @staticmethod
    # def volatility_all():
    #     warnings.warn("deprecated", DeprecationWarning)
    #     return Owner.client.read_frame(measurement="VolatilityOwner", field="volatility", tags=["owner"])
    #
    # @staticmethod
    # def position_all():
    #     warnings.warn("deprecated", DeprecationWarning)
    #     x = Owner.client.read_series(measurement="WeightsOwner", field="weight",
    #                                  tags=["owner", "security", "custodian"])
    #     return x.reorder_levels(["owner", "security", "custodian", "time"])
