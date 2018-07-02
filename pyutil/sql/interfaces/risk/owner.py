import pandas as pd
import sqlalchemy as _sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship


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
    "Inception Date": Field(name="Inception Date", result=DataType.string, type=FieldType.other)  # don't use date here...
}

def date2str(x):
    return x.strftime("%Y-%m-%d")


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

    def returns(self, client):
        return client.series(field="returns", measurement="owner", conditions=[("owner", self.name)], date=True)

    def volatility(self, client):
        return client.series(field="volatility", measurement="owner", conditions=[("owner", self.name)], date=True)

    def position(self, client, sum=False, tail=None):
        f = client.frame(field="weight", tags=["security"], measurement="owner", date=True)

        if tail:
            f = f.tail(tail)

        f = f.transpose()

        if sum:
            f.loc["Sum"] = f.sum(axis=0)

        return f


    def position_by(self, client, sum=False, tail=None, index_col=None):
        if index_col:
            pos = self.position(client=client, sum=False, tail=tail)
            try:
                ref = self.reference_securities[index_col]
                f = pd.concat((pos, ref), axis=1)
                a = f.groupby(by=index_col).sum()
                if sum:
                    a.loc["Sum"] = a.sum(axis=0)
                return a

            except KeyError:
                return pd.DataFrame({})
        else:
            pos = self.position(client=client, sum=sum, tail=tail)
            ref = self.reference_securities
            return pd.concat((pos, ref), axis=1, sort=True)


    #def position_by(self, client, index_col=None, sum=False, tail=None):
    #    if index_col:
    #        try:
    #            ref = self.reference_securities[index_col]
    #        except KeyError:
    #            ref = pd.DataFrame({})
    #    else:
    #        ref = self.reference_securities

    #    x = pd.concat((self.position(client), ref), axis=1)



        #x = pd.concat((self.position(client, sum=sum), self.reference_securities), axis=1)

    #    return x

        #return self.__weighted_by(client, f = self.position, index_col=index_col, sum=sum, tail=tail)

    # def __weighted_by(self, client, f, index_col=None, sum=False, tail=None):
    #     if index_col:
    #         if index_col in self.reference_securities.keys():
    #             a = pd.concat((f(client, sum=False, tail=tail), self.reference_securities[index_col]), axis=1)
    #             a = a.groupby(by=index_col).sum()
    #             if sum:
    #                 a.loc["Sum"] = a.sum(axis=0)
    #             return a
    #         else:
    #             return pd.DataFrame({})
    #
    #     return pd.concat((f(sum=sum, tail=tail), self.reference_securities), axis=1)

    def vola_securities(self, client):
        f = client.frame(field="volatility", measurement="securities", tags=["security"], conditions=[("currency",self.currency.name)])
        print(f)

        x = pd.DataFrame({security.name: security.volatility(client=client, currency=self.currency.name) for security in self.securities})
        return x.transpose()

    def vola_weighted(self, client, sum=False, tail=None):
        x = self.position(client, sum=False, tail=tail).multiply(self.vola_securities(client=client)).dropna(axis=0, how="all").dropna(axis=1, how="all")
        if sum:
            x.loc["Sum"] = x.sum(axis=0)

        return x


    def vola_weighted_by(self, client, index_col=None, sum=False, tail=None):
        if index_col:
            vola = self.vola_weighted(client=client, sum=False, tail=tail)
            try:
                ref = self.reference_securities[index_col]
                f = pd.concat((vola, ref), axis=1)
                a = f.groupby(by=index_col).sum()
                if sum:
                    a.loc["Sum"] = a.sum(axis=0)
                return a

            except KeyError:
                return pd.DataFrame({})
        else:
            vola = self.vola_weighted(client=client, sum=sum, tail=tail)
            ref = self.reference_securities
            return pd.concat((vola, ref), axis=1, sort=True)

    @property
    def reference_securities(self):
        return pd.DataFrame({security.name: security.reference_series.sort_index() for security in self.securities}).sort_index().transpose()

    #@property
    #def current_position(self):
    #    p = self.position(sum=False).transpose().ffill()
    #    if len(p.index) >= 1:
    #        return p.loc[p.index[-1]].rename(None)
    #    else:
    #        return None

    @property
    def kiid(self):
        return pd.Series({security.name: security.kiid for security in self.securities})

    def kiid_weighted(self, client, sum=False, tail=None):
        x = self.position(client=client, sum=False, tail=tail).apply(lambda weights: weights * self.kiid, axis=0).dropna(axis=0, how="all")
        if sum:
            x.loc["Sum"] = x.sum(axis=0)
        return x

    def kiid_weighted_by(self, client, index_col=None, sum=False, tail=None):
        if index_col:
            kiid = self.kiid_weighted(client=client, sum=False, tail=tail)
            try:
                ref = self.reference_securities[index_col]
                f = pd.concat((kiid, ref), axis=1)
                a = f.groupby(by=index_col).sum()
                if sum:
                    a.loc["Sum"] = a.sum(axis=0)
                return a

            except KeyError:
                return pd.DataFrame({})
        else:
            kiid = self.kiid_weighted(client=client, sum=sum, tail=tail)
            ref = self.reference_securities
            return pd.concat((kiid, ref), axis=1, sort=True)


        #return self.__weighted_by(f = self.kiid_weighted, index_col=index_col, sum=sum, tail=tail)
    #
    #
    # @property
    # def nav(self):
    #     return _NavSeries(self.get_timeseries("nav"))

    def upsert_return(self, client, ts):
        # client is the influx client
        if len(ts) > 0:
            helper = client.helper(tags=["owner"], fields=["returns"], series_name='owner', autocommit=True, bulk_size=10)

            for date, value in ts.items():
                helper(owner=self.name, returns=value, time=date)

            helper.commit()


    def upsert_position(self, client, security, custodian, ts):
        if len(ts) > 0:
            if security not in self.__securities:
                self.__securities.append(security)

            helper = client.helper(tags=["owner", "security", "custodian"], fields=["weight"], series_name='owner',
                          autocommit=True, bulk_size=10)

            for date, value in ts.items():
                helper(owner=self.name, security=security.name, custodian=custodian, time=date, weight=value)

            helper.commit()


    def upsert_volatility(self, client, ts):
        if len(ts) > 0:
            helper = client.helper(tags=["owner"], fields=["volatility"], series_name='owner', autocommit=True, bulk_size=10)

            for date, value in ts.items():
                helper(owner=self.name, volatility=value, time=date)

            helper.commit()

