from pyutil.sql.db import Database
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.util import to_pandas, reference


class DatabaseRisk(Database):
    def __init__(self, client, session=None):
        super().__init__(session=session, db="addepar2")
        self.__client = client

    def owner(self, name):
        return self.session.query(Owner).filter_by(name=str(name)).one()

    def security(self, name):
        return self.session.query(Security).filter_by(name=str(name)).one()

    @property
    def prices(self):
        return self.__client.frame(measurement="security", field="price", tags=["security"], date=True)

    @property
    def returns(self):
        return self.__client.frame(measurement="owner", field="returns", tags=["owner"], date=True)

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
        return self.__client.frame(measurement="owner", field="weight", tags=["owner", "custodian", "security"], date=True).stack()

    @property
    def volatility_owner(self):
        return self.__client.frame(measurement="owner", field="volatility", tags=["owner"], date=True)

    @property
    def volatility_security(self):
        return self.__client.frame(measurement="security", field="volatility", tags=["currency", "security"], date=True).swaplevel()

    #@property
    #def volatility_owner_securities(self):
    #    return self.__client.frame(measurement="")
    #    vola = self._read(sql="SELECT * FROM v_volatility_owner_security", index_col=["owner", "security"])["data"]
    #    return vola.apply(to_pandas)



