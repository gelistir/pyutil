import pandas as pd

import pandas as _pd
import sqlalchemy as _sq
from pyutil.performance.summary import NavSeries as _NavSeries
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship
from pyutil.sql.interfaces.products import ProductInterface, association_table, Products
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security

_association_table = association_table(left="security", right="owner", name="security_owner")


FIELDS = {
    "name": Field(name="Name", result=DataType.string, type=FieldType.other),
    "15. Custodian Name": Field(name="Custodian", result=DataType.string, type=FieldType.other),
    "17. Reference Currency": Field(name="Currency", result=DataType.string, type=FieldType.other),
    "18. LWM Risk Profile": Field(name="Risk Profile", result=DataType.string, type=FieldType.other),
    "23. LWM - AUM Type": Field(name="AUM Type", result=DataType.string, type=FieldType.other),
    "Inception Date": Field(name="Inception Date", result=DataType.string, type=FieldType.other)  # don't use date here...
}


class Owner(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Owner"}
    __entity_id = _sq.Column("entity_id", _sq.Integer, nullable=False)
    __securities = _relationship(Security, secondary=_association_table, backref="owner", lazy="joined")
    __currency_id = _sq.Column("currency_id", _sq.Integer, _sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    def __init__(self, name, currency):
        super().__init__(name=name)
        self.__entity_id = name
        self.__currency = currency

    @hybrid_property
    def currency(self):
        return self.__currency

    @property
    def securities(self):
        return self.__securities

    @property
    def returns(self):
        return self.timeseries["return"]

    def returns_upsert(self, ts):
        self.upsert_ts(name="return", data=ts)

    def position_upsert(self, security, ts):
        if security not in self.__securities:
            self.__securities.append(security)

        self.upsert_ts("position", data=ts, secondary=security)

    def volatility_upsert(self, ts):
        self.upsert_ts(name="volatility", data=ts)

    @property
    def volatility(self):
        return self.timeseries["volatility"]

    @property
    def position(self):
        return self.frame(name="position")

    def position_by(self, index=None):
        if index:
            a = _pd.concat((self.position.transpose(), self.reference_securities[index]), axis=1)
            a = a.groupby(by=index).sum()
            return a.rename(columns=lambda x: pd.Timestamp(x))

        return _pd.concat((self.position.transpose(), self.reference_securities), axis=1)

    def vola_securities(self):
        return _pd.DataFrame({security: security.volatility[self.currency] for security in self.securities})

    def vola_weighted(self):
        return self.position.multiply(self.vola_securities()).dropna(axis=0, how="all").dropna(axis=1, how="all")

    def vola_weighted_by(self, index=None):
        if index:
            a = _pd.concat((self.vola_weighted().transpose(), self.reference_securities[index]), axis=1)
            a = a.groupby(by=index).sum()
            return a.rename(columns=lambda x: pd.Timestamp(x))

        return _pd.concat((self.vola_weighted().transpose(), self.reference_securities), axis=1)

    @property
    def reference_securities(self):
        y = _pd.DataFrame({security: _pd.Series(dict(security.reference)).sort_index() for security in self.securities})
        y = y.rename(index=lambda f: f.name).sort_index()
        return y.transpose()

    @property
    def current_position(self):
        p = self.position.ffill()
        if len(p.index) >= 1:
            return p.loc[p.index[-1]].rename(None)
        else:
            return None

    @property
    def kiid(self):
        return _pd.Series({security: security.kiid for security in self.securities})

    @property
    def kiid_weighted(self):
        return self.position.apply(lambda a: a * self.kiid, axis=1).dropna(axis=0, how="all")

    @property
    def nav(self):
        try:
            x = self.returns
            x = _pd.concat((_pd.Series({(x.index[0] - _pd.DateOffset(days=1)).date(): 0.0}), x))
            # ts = x.loc[_pd.notnull(x.index)]
            return _NavSeries((x + 1.0).cumprod())
        except:
            return _NavSeries(_pd.Series({}))


class Owners(Products):
    def __init__(self, owners):
        super().__init__(owners, cls=Owner, attribute="name")
