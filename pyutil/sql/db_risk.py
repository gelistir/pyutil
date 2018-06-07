import pandas as pd

from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.session import session as _session
from pyutil.sql.util import to_pandas, parse


class Database(object):
    def __init__(self, session=None):
        self.__session = session or _session(db="addepar2")

    def owner(self, name):
        return self.__session.query(Owner).filter_by(name=name).one()

    def security(self, name):
        return self.__session.query(Security).filter_by(name=str(name)).one()

    @property
    def prices(self):
        prices = pd.read_sql_query(sql="SELECT * FROM v_prices", con=self.__session.bind, index_col=["security"])["data"]
        return prices.apply(to_pandas)

    @property
    def reference_owner(self):
        frame = pd.read_sql_query("SELECT * FROM v_reference_owner", con=self.__session.bind, index_col=["owner", "field"])
        if frame.empty:
            return pd.DataFrame(index=frame.index, columns=["value"])

        frame["value"] = frame[['result', 'content']].apply(lambda x: parse(x[1], x[0]), axis=1)
        return frame["value"].unstack()

    @property
    def reference_securities(self):
        frame = pd.read_sql_query("SELECT * FROM v_reference_securities", con=self.__session.bind, index_col=["security", "field"])
        if frame.empty:
            return pd.DataFrame(index=frame.index, columns=["value"])

        frame["value"] = frame[['result', 'content']].apply(lambda x: parse(x[1], x[0]), axis=1)
        return frame["value"].unstack()

    @property
    def reference_owner_securities(self):
        frame = pd.read_sql_query("SELECT * FROM v_reference_owner_securities", con=self.__session.bind, index_col=["owner", "security", "field"])
        if frame.empty:
            return pd.DataFrame(index=frame.index, columns=["value"])

        frame["value"] = frame[['result', 'content']].apply(lambda x: parse(x[1], x[0]), axis=1)
        return frame["value"].unstack()

    @property
    def position(self):
        # read all positions at once. This is fast!
        position = \
        pd.read_sql_query(sql="SELECT * FROM v_position", con=self.__session.bind, index_col=["owner", "security"])[
            "data"]
        return position.apply(to_pandas)

    @property
    def volatility_owner(self):
        vola = pd.read_sql_query(sql="SELECT * FROM v_volatility_owner", con=self.__session.bind, index_col=["owner"])[
            "data"]
        return vola.apply(to_pandas).sort_index()

    @property
    def volatility_security(self):
        vola = pd.read_sql_query(sql="SELECT * FROM v_volatility_security", con=self.__session.bind,
                                 index_col=["currency", "security"])["data"]
        return vola.apply(to_pandas)

    @property
    def volatility_owner_securities(self):
        vola = pd.read_sql_query(sql="SELECT * FROM v_volatility_owner_security", con=self.__session.bind,
                                 index_col=["owner", "security"])["data"]
        return vola.apply(to_pandas)
