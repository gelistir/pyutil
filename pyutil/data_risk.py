#import pandas as pd

from pyutil.sql.interfaces.products import ProductInterface
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

    def owner(self, name):
        return self.owners.filter(Owner.name == name).one()

    def security(self, name):
        return self.securities.filter(Security.name == name).one()

    @property
    def reference_owner(self):
        return Owner.reference_frame(self.owners)

    @property
    def reference_securities(self):
        return Security.reference_frame(self.securities)


    #def sector(self, total=False):
    #    return pd.DataFrame({p.name: p.sector(total=total).iloc[-1] for p in self.portfolios}).transpose()

    #def nav(self, f=None):
    #    f = f or (lambda x: x)

    #    # we prefer this solution as is goes through the cleaner SQL database!
    #    return pd.DataFrame({portfolio.name: f(portfolio.nav) for portfolio in self.portfolios})

    #def frame(self, name):
    #    return self.session.query(Frame).filter_by(name=name).one()