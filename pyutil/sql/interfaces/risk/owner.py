import pandas as pd
import sqlalchemy as _sq
from pyutil.performance.summary import NavSeries as _NavSeries, fromNav
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship
from pyutil.sql.interfaces.products import ProductInterface, association_table, Products
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security

#from pyutil.web.aux import double2percent
#from pandasweb.frames import frame2dict

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
    __securities = _relationship(Security, secondary=_association_table, backref="owner", lazy="joined")
    __currency_id = _sq.Column("currency_id", _sq.Integer, _sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    def __init__(self, name, currency):
        super().__init__(name=name)
        self.__currency = currency

    def __repr__(self):
        return "Owner({id}: {name})".format(id=self.name, name=self.get_reference(field=FIELDS["name"]))

    @hybrid_property
    def currency(self):
        return self.__currency

    @property
    def securities(self):
        return self.__securities

    @property
    def returns(self):
        return self.get_timeseries("return")

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
        return self.get_timeseries("volatility")

    @property
    def position(self):
        frame = self.frame(name="position", rename=True)
        frame = frame.rename(index=lambda t: t.date())
        frame = frame.transpose()
        return frame


    def position_by(self, index=None):
        if index:
            assert isinstance(index, str), "Index has to be a string"
            a = pd.concat((self.position, self.reference_securities[index]), axis=1)
            a = a.groupby(by=index).sum()
            return a
            #return a.rename(columns=lambda x: pd.Timestamp(x).date())
        #print(self.position)
        #print(type(self.position.columns[0]))
        #print(self.reference_securities)
        a = pd.concat((self.position, self.reference_securities), axis=1)
        #print(a)
        #assert False

        return a

    @property
    def vola_securities(self):
        x = pd.DataFrame({security: security.volatility[self.currency] for security in self.securities})
        x = x.rename(index=lambda t: t.date())
        return x.rename(columns=lambda x: x.name).transpose()

    @property
    def vola_weighted(self):
        return self.position.multiply(self.vola_securities).dropna(axis=0, how="all").dropna(axis=1, how="all")

    def vola_weighted_by(self, index=None):
        if index:
            a = pd.concat((self.vola_weighted, self.reference_securities[index]), axis=1)
            a = a.groupby(by=index).sum()
            return a
            #return a.rename(columns=lambda x: pd.Timestamp(x).date())

        return pd.concat((self.vola_weighted, self.reference_securities), axis=1)

    @property
    def reference_securities(self):
        y = pd.DataFrame({security: pd.Series(dict(security.reference_series)).sort_index() for security in self.securities})
        return y.rename(columns=lambda f: f.name).sort_index().transpose()

    @property
    def current_position(self):
        p = self.position.transpose().ffill()
        if len(p.index) >= 1:
            return p.loc[p.index[-1]].rename(None)
        else:
            return None

    @property
    def kiid(self):
        x = pd.Series({security: security.kiid for security in self.securities})
        return x.rename(index=lambda x: x.name)

    @property
    def kiid_weighted(self):
        return self.position.apply(lambda a: a * self.kiid, axis=0).dropna(axis=0, how="all")

    @property
    def nav(self):
        try:
            x = self.returns
            x = pd.concat((pd.Series({(x.index[0] - pd.DateOffset(days=1)).date(): 0.0}), x))
            # ts = x.loc[_pd.notnull(x.index)]
            return _NavSeries((x + 1.0).cumprod())
        except:
            return _NavSeries(pd.Series({}))

    def to_html_dict(self):
        return fromNav(ts=self.nav, adjust=False).to_dictionary()


class Owners(Products):
    def __init__(self, owners):
        super().__init__(owners, cls=Owner, attribute="name")
