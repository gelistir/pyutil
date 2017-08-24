import pandas as pd
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets


class Database(object):
    def __init__(self, connection):
        self.__con = connection

    def __history_sql(self, assets=None, names=None):
        cond = []
        if names:
            cond.append("ts_name in {names}".format(names=tuple(names)).replace(',)', ')'))

        if assets:
            cond.append("bloomberg_symbol in {assets}".format(assets=tuple(assets)).replace(',)', ')'))

        w = "WHERE {c}".format(c=" AND ".join(cond)) if cond else ""

        x = pd.read_sql("SELECT * FROM ts_data_complete {cond}".format(cond=w), con=self.__con,
                        index_col=["bloomberg_symbol", "ts_name", "date"])["value"]
        x = x.unstack(level=["ts_name", "bloomberg_symbol"])
        return x

    def __reference_sql(self, names=None, assets=None):
        cond = []
        if names:
            cond.append("name in {names}".format(names=tuple(names)).replace(',)', ')'))

        if assets:
            cond.append("bloomberg_symbol in {assets}".format(assets=tuple(assets)).replace(',)', ')'))

        w = "WHERE {c}".format(c=" AND ".join(cond)) if cond else ""

        x = pd.read_sql("SELECT * FROM reference_data {cond}".format(cond=w), con=self.__con,
                        index_col=["bloomberg_symbol", "name"])["content"]
        x = x.unstack(level=["name"])
        return x

    def asset(self, name):
        data = self.__history_sql(assets=[name])
        data.columns = data.columns.droplevel(level=1)

        rdata = self.__reference_sql(assets=[name])
        x = rdata.transpose()[name].to_dict()
        return Asset(name=name, data=data, **x)

    @property
    def reference(self):
        return self.__reference_sql()

    def history(self, name="PX_LAST"):
        return self.__history_sql(names=[name])[name]

    def assets(self, names):
        return Assets({name: self.asset(name) for name in names})


if __name__ == '__main__':
    from pyutil.sql.io import postgresql
    db = Database(connection=postgresql(conn_str="..."))
    asset = db.asset(name="CARMPAT FP Equity")
    print(asset.reference)
    print(asset.time_series)