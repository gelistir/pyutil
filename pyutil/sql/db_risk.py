import logging

from pyutil.sql.db import Database
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.util import to_pandas, reference


class DatabaseRisk(Database):
    def __init__(self, session=None):
        super().__init__(db="addepar2", session=session)

    def owner(self, name):
        return self.session.query(Owner).filter_by(name=str(name)).one()

    def security(self, name):
        return self.session.query(Security).filter_by(name=str(name)).one()

    @property
    def prices(self):
        return self._read(sql="SELECT * FROM v_prices", index_col=["security"])["data"].apply(to_pandas)

    @property
    def reference_owner(self):
        return reference(self._read("SELECT * FROM v_reference_owner", index_col=["owner", "field"]))

    @property
    def reference_securities(self):
        return reference(self._read("SELECT * FROM v_reference_securities", index_col=["security", "field"]))

    @property
    def reference_owner_securities(self):
        return reference(self._read("SELECT * FROM v_reference_owner_securities", index_col=["owner", "security", "field"]))

    @property
    def position(self):
        # read all positions at once. This is fast!
        return self._read(sql="SELECT * FROM v_position", index_col=["owner", "security"])["data"].apply(to_pandas)

    @property
    def volatility_owner(self):
        return self._read(sql="SELECT * FROM v_volatility_owner", index_col=["owner"])["data"].apply(to_pandas).sort_index()

    @property
    def volatility_security(self):
        vola = self._read(sql="SELECT * FROM v_volatility_security", index_col=["currency", "security"])["data"]
        return vola.apply(to_pandas)

    @property
    def volatility_owner_securities(self):
        vola = self._read(sql="SELECT * FROM v_volatility_owner_security", index_col=["owner", "security"])["data"]
        return vola.apply(to_pandas)


class Runner(object):
    def __init__(self, session, logger=None):
        self.__db = DatabaseRisk(session)
        self.__logger = logger or logging.getLogger(__name__)

    @property
    def database(self):
        return self.__db

    @property
    def logger(self):
        return self.__logger

