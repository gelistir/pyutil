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

    def f(self, match):
        for a in self.ts.keys():
            if a.startswith(match):
                yield tuple(a.split("_")[1:]), self.ts[a]

    @property
    def __position(self):

        d = dict()
        for tags, data in self.f("position"):
            d[tags] = data

        a = pd.DataFrame(d).transpose().stack()
        a.index.names = ["Security", "Custodian", "Date"]
        return a

    def position(self, index_col=None):
        if not index_col:
            return self.__position
        else:
            p = self.__position.unstack(level="Security")

            g = self.reference_securities
            g.index = [x.name for x in g.index]

            for security in p.keys():
                p[security] = p[security] * g.loc[security][index_col]

            return p.stack()

    @property
    def __volatility(self):
        x = pd.DataFrame({security.name: security.volatility(currency=self.currency) for security in set(self.securities)}).stack()
        x.index.names = ["Date", "Security"]
        print(x)

        return x

    def vola(self, index_col=None):
        if not index_col:
            return self.__volatility
        else:
            v = self.__volatility.unstack(level="Security")

            g = self.reference_securities
            g.index = [x.name for x in g.index]

            for security in v.keys():
                v[security] = v[security] * g.loc[security][index_col]

            return v.stack()

    def vola_weighted(self, index_col=None):
        v = self.vola(index_col=index_col)
        print(index_col)
        print(v)
        # date security
        #assert False

        # get rid of the custodian here...
        w = self.position().groupby(level=["Date", "Security"]).sum()
        print(w)
        #assert False

        return w*v


    @property
    def kiid(self):
        return pd.Series({security: security.kiid for security in self.securities})

    def upsert_position(self, security, ts, custodian=None):
        assert isinstance(security, Security)

        # append the security. This is an idempotent operation! Not really!?
        if not security in set(self.securities):
            self.securities.append(security)

        # if not custodian:
        custodian = custodian or self.custodian

        assert isinstance(custodian, Custodian)

        self.ts["position_{sec}_{custodian}".format(sec=security.name, custodian=custodian.name)] = ts

    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities)
