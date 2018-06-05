import pandas as pd
import sqlalchemy as _sq
from pandasweb.frames import frame2dict

from pyutil.performance.summary import NavSeries as _NavSeries, fromNav
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

def date2str(x):
    return x.strftime("%Y-%m-%d")


class Owner(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Owner"}
    __securities = _relationship(Security, secondary=_association_table, backref="owner", lazy="joined")
    __currency_id = _sq.Column("currency_id", _sq.Integer, _sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    def __init__(self, name, currency):
        super().__init__(name=name)
        self.__currency = currency

    def __repr__(self):
        return "Owner({id}: {name})".format(id=self.name, name=self.get_reference("Name"))

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

    def position(self, sum=False, tail=None, rename=True):
        frame = self.frame(name="position", rename=rename)

        if tail:
            frame=frame.tail(n=tail)

        frame = frame.rename(index=lambda t: date2str(t)).transpose()
        frame.index.names = ["Asset"]

        if sum:
            frame.loc["Sum"] = frame.sum(axis=0)

        return frame

    def position_by(self, index_col=None, sum=False, tail=None):
        return self.__weighted_by(f = self.position, index_col=index_col, sum=sum, tail=tail)

    def __weighted_by(self, f, index_col=None, sum=False, tail=None):
        if index_col:
            if index_col in self.reference_securities.keys():
                a = pd.concat((f(sum=False, tail=tail), self.reference_securities[index_col]), axis=1)
                a = a.groupby(by=index_col).sum()
                if sum:
                    a.loc["Sum"] = a.sum(axis=0)
                return a
            else:
                return pd.DataFrame({})

        return pd.concat((f(sum=sum, tail=tail), self.reference_securities), axis=1)

    @property
    def vola_securities(self):
        x = pd.DataFrame({security.name: security.volatility[self.currency] for security in self.securities})
        return x.rename(index=lambda t: date2str(t)).transpose()

    def vola_weighted(self, sum=False, tail=None):
        x = self.position(sum=False, tail=tail).multiply(self.vola_securities).dropna(axis=0, how="all").dropna(axis=1, how="all")
        if sum:
            x.loc["Sum"] = x.sum(axis=0)

        return x


    def vola_weighted_by(self, index_col=None, sum=False):
        return self.__weighted_by(f = self.vola_weighted, index_col=index_col, sum=sum)

    @property
    def reference_securities(self):
        return pd.DataFrame({security.name: security.reference_series.sort_index() for security in self.securities}).sort_index().transpose()

    @property
    def current_position(self):
        p = self.position(sum=False).transpose().ffill()
        if len(p.index) >= 1:
            return p.loc[p.index[-1]].rename(None)
        else:
            return None

    @property
    def kiid(self):
        return pd.Series({security.name: security.kiid for security in self.securities})

    def kiid_weighted(self, sum=False, tail=None):
        x = self.position(sum=False, tail=tail).apply(lambda weights: weights * self.kiid, axis=0).dropna(axis=0, how="all")
        if sum:
            x.loc["Sum"] = x.sum(axis=0)
        return x

    def kiid_weighted_by(self, index_col=None, sum=False, tail=None):
        return self.__weighted_by(f = self.kiid_weighted, index_col=index_col, sum=sum, tail=tail)


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
        def double2percent(x):
            return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

        w = self.position(sum=False).applymap(double2percent).reset_index()
        #w["Asset"] = w["Asset"].apply(str)
        return fromNav(ts=self.nav, adjust=False).to_dictionary(name=self.get_reference("Name"), weights=frame2dict(w))


# class Owners(Products):
#     def __init__(self, owners):
#         super().__init__(owners, cls=Owner, attribute="name", f=lambda x: int(x))
#
#     def __repr__(self):
#         d = {key: product for key, product in self.to_dict().items()}
#         seq = ["{key:10d}   {product}".format(key=key, product=d[key]) for key in sorted(d)]
#         return "\n".join(seq)
#
#     def to_html_dict(self, index_name="Entity ID"):
#         return self.to_html(index_name=index_name)

    #@property
    #def returns(self):
    #    return pd.DataFrame({owner.get_reference("Name") : owner.returns for owner in self}).dropna(axis=1, how="all")

    #@property
    #def positions(self):
    #    frame = pd.concat({o.get_reference("Name"): o.position().stack() for o in self}, axis=0)
    #    frame = frame.to_frame(name="Weight")
    #    frame.index.names = ["Owner", "Asset", "Date"]
    #    return frame

    #@property
    #def volatility(self):
    #    return pd.DataFrame({o.get_reference("Name"): o.volatility for o in self}).transpose().dropna(axis=0, how="all")


