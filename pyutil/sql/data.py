import pandas as pd
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from pyutil.portfolio.portfolio import Portfolio


class Database(object):
    def __init__(self, connection):
        self.__con = connection

    def __strat_id(self, strategy_name):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM strategy WHERE name=%(strategy)s;", {"strategy": strategy_name})
            return cursor.fetchone()[0]

    def __symbol_id(self, ticker):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM asset WHERE bloomberg_symbol=%(ticker)s;", {"ticker": ticker})
            return cursor.fetchone()[0]

    def __ts_id(self, ticker, field):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM ts_vs_asset WHERE bloomberg_symbol=%(ticker)s::integer AND ts_name=%(field)s;",
                           {"ticker": ticker, "field": field})
            return cursor.fetchone()[0]

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

    def history(self, name="PX_LAST", assets=None):
        frame = self.__history_sql(names=[name], assets=assets)
        if not frame.empty:
            return frame[name]
        else:
            return frame

    def assets(self, names):
        return Assets({name: self.asset(name) for name in names})

    @property
    def strategies(self):
        return pd.read_sql("SELECT * FROM strategy", con=self.__con, index_col="name")

    def portfolio(self, name):
        index = ["date", "bloomberg_symbol"]
        f = lambda x: "SELECT date, bloomberg_symbol, {field} FROM strategy_data WHERE name='{strategy}'".format(strategy=name, field=x)

        price = pd.read_sql(f("price"), con=self.__con, index_col=index).unstack(level=1)["price"]
        weight = pd.read_sql(f("weight"), con=self.__con, index_col=index).unstack(level=1)["weight"]
        return Portfolio(prices=price, weights=weight)


    def upsert_portfolio(self, portfolio, name):

        upsert = """
                    INSERT INTO "strat_data"
                    (date, asset_id, strategy_id, price, weight)
                    VALUES 
                    (%(date)s, %(asset_id)s::INTEGER, %(strategy_id)s::INTEGER, %(price)s, %(weight)s)
                    ON CONFLICT ON CONSTRAINT time_asset_strategy
                    DO UPDATE SET price=%(price)s, weight=%(weight)s
                    WHERE 
                      strat_data.strategy_id=%(strategy_id)s AND 
                      strat_data.date=%(date)s AND 
                      strat_data.asset_id=%(asset_id)s
                 """

        strategy_id = self.__strat_id(name)

        with self.__con.cursor() as cursor:
            for asset in portfolio.assets:
                asset_id = self.__symbol_id(ticker=asset)

                for date in portfolio.index:
                    price = portfolio.prices.loc[date][asset]
                    weight = portfolio.weights.loc[date][asset]
                    data = {"date": date, "asset_id": asset_id, "strategy_id": strategy_id, "price": price, "weight": weight}
                    cursor.execute(upsert, data)