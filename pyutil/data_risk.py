import pandas as pd

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security


class Database(object):
    def __init__(self, session):
        self.__session = session

    def close(self):
        self.__session.close()

    @property
    def session(self):
        return self.__session

    @property
    def owners(self):
        return self.__session.query(Owner)

    @property
    def securities(self):
        return self.__session.query(Security)

    @property
    def currencies(self):
        return self.__session.query(Currency)

    @property
    def custodians(self):
        return self.__session.query(Custodian)

    def owner(self, name):
        return self.owners.filter(Owner.name == name).one()

    def security(self, name):
        return self.securities.filter(Security.name == name).one()

    def currency(self, name):
        return self.currencies.filter(Currency.name == name).one()

    def custodian(self, name):
        return self.custodians.filter(Custodian.name == name).one()

    @property
    def reference_owners(self):
        return Owner.reference_frame(self.owners, name="Entity ID").reset_index()

    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities, name="Entity ID").reset_index()

    @property
    def prices(self):
        return pd.DataFrame({security.name: security.price for security in self.securities if security.price is not None})

    @property
    def returns(self):
        return pd.DataFrame({owner.name: owner.returns for owner in self.owners if owner.returns is not None})

    @property
    def owner_volatility(self):
        return pd.DataFrame({owner.name: owner.volatility for owner in self.owners if owner.volatility is not None})

    def securities_volatility(self, currency):
        return pd.DataFrame({security.name: security.vola.get(currency, pd.Series({})) for security in self.securities})

