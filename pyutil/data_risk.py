import pandas as pd

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security


class Database(object):

    def __init__(self, session, client=None):
        # session, sql database
        self.__session = session
        if client:
            self.__client = client
            # this is how the sql database is using influx...
            ProductInterface.client = self.__client

    def close(self):
        self.__session.close()
        if self.__client:
            self.__client.close()

    @property
    def influx_client(self):
        return self.__client

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
        return Owner.reference_frame(self.owners)

    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities)

    @property
    def prices(self):
        return pd.DataFrame({security.name: security.price for security in self.securities})

    @property
    def returns(self):
        return pd.DataFrame({owner.name: owner.returns for owner in self.owners})

    @property
    def owner_volatility(self):
        return pd.DataFrame({owner.name: owner.volatility for owner in self.owners})

    def positions(self, owner, index_col=None):
        #for owner in self.session.query(Owner):
        for x in owner.position(index_col=index_col):
            yield x

    def securities_volatility(self, currency):
        #for security in self.securities:
        return pd.DataFrame({security.name: security.volatility(currency) for security in self.securities})


        #    yield security.name, security.volatility(currency)
    #def sector(self, total=False):
    #    return pd.DataFrame({p.name: p.sector(total=total).iloc[-1] for p in self.portfolios}).transpose()

    #def nav(self, f=None):
    #    f = f or (lambda x: x)

    #    # we prefer this solution as is goes through the cleaner SQL database!
    #    return pd.DataFrame({portfolio.name: f(portfolio.nav) for portfolio in self.portfolios})

    #def frame(self, name):
    #    return self.session.query(Frame).filter_by(name=name).one()