import pandas as pd
import numpy as np

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security


class Database(object):
    @staticmethod
    def _f(series):
        if series is not None and not series.empty:
            return series.dropna()
        else:
            return np.nan

    def __init__(self, session):
        self.__session = session

    def close(self):
        self.session.close()

    @property
    def session(self):
        return self.__session

    def __repr__(self):
        return str(self.session)

    @property
    def owners(self):
        return self.session.query(Owner)

    @property
    def securities(self):
        return self.session.query(Security)

    @property
    def currencies(self):
        return self.session.query(Currency)

    @property
    def custodians(self):
        return self.session.query(Custodian)

    def owner(self, name):
        return self.owners.filter(Owner.name == name).one()

    def security(self, name):
        return self.securities.filter(Security.name == name).one()

    def currency(self, name):
        return self.currencies.filter(Currency.name == name).one()

    def custodian(self, name):
        return self.custodians.filter(Custodian.name == name).one()

    def reference_owners(self, owners=None):
        owners = owners or self.owners
        return Owner.reference_frame(owners, name="Entity ID").reset_index()

    def reference_securities(self, securities=None):
        sec = securities or self.securities
        return Security.reference_frame(sec, name="Entity ID").reset_index()

    def prices(self, securities=None):
        sec = securities or self.securities
        return pd.DataFrame({security.name: Database._f(security.price) for security in sec}).dropna(axis=1, how="all")

    @property
    def returns(self):
        return pd.DataFrame({owner.name: Database._f(owner.returns) for owner in self.owners}).dropna(axis=1, how="all")

    @property
    def owner_volatility(self):
        return pd.DataFrame({owner.name: Database._f(owner.volatility) for owner in self.owners}).dropna(axis=1, how="all")

    def securities_volatility(self, currency):
        return pd.DataFrame({security.name: Database._f(security.volatility(currency)) for security in self.securities}).dropna(axis=1, how="all")