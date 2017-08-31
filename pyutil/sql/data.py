import pandas as pd
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from pyutil.portfolio.portfolio import Portfolio


class Database(object):
    def __init__(self, connection):
        self.__con = connection

    def _strat_id(self, strategy_name):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM strategy WHERE name=%(strategy)s;", {"strategy": strategy_name})
            f = cursor.fetchone()
            assert f is not None, "The strategy {strategy_name} does not exist in the database".format(ticker=ticker)
            return f[0]

    def _symbol_id(self, ticker):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM asset WHERE bloomberg_symbol=%(ticker)s;", {"ticker": ticker})
            f = cursor.fetchone()
            assert f is not None, "The ticker {ticker} does not exist in the database".format(ticker=ticker)
            return f[0]

    def _field_id(self, field):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM reference_fields WHERE field=%(field)s;", {"field": field})
            f = cursor.fetchone()
            assert f is not None, "The field {field} does not exist in the database".format(field=field)
            return f[0]

    def _ts_id(self, ticker, field):
        with self.__con.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM ts_vs_asset WHERE bloomberg_symbol=%(ticker)s AND ts_name=%(field)s;",
                {"ticker": ticker, "field": field})
            f = cursor.fetchone()
            assert f is not None, "The timeseries for {ticker} and field {field} does not exist in the database".format(ticker=ticker, field=field)
            return f[0]


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

    @property
    def ticker(self):
        return pd.read_sql("SELECT * FROM asset", con=self.__con, index_col="bloomberg_symbol")

    def portfolio(self, name):
        index = ["date", "bloomberg_symbol"]
        f = lambda x: "SELECT date, bloomberg_symbol, {field} FROM strategy_data WHERE name='{strategy}'".format(
            strategy=name, field=x)

        price = pd.read_sql(f("price"), con=self.__con, index_col=index).unstack(level=1)["price"]
        weight = pd.read_sql(f("weight"), con=self.__con, index_col=index).unstack(level=1)["weight"]
        return Portfolio(prices=price, weights=weight)

    def timeseries(self, asset, name):
        return pd.read_sql("SELECT date, value FROM ts_data_complete WHERE bloomberg_symbol='{asset}' "
                        "AND ts_name='{name}'".format(asset=asset, name=name), con=self.__con,
                        index_col=["date"])["value"]

    def upsert_portfolio(self, portfolio, name):
        upsert = """
                    INSERT INTO "strat_data"
                    (date, asset_id, strategy_id, price, weight)
                    VALUES 
                    (%(date)S, %(asset_id)S::INTEGER, %(strategy_id)S::INTEGER, %(price)S, %(weight)S)
                    ON CONFLICT ON CONSTRAINT time_asset_strategy
                    DO UPDATE SET price=%(price)S, weight=%(weight)S
                    WHERE 
                      strat_data.strategy_id=%(strategy_id)S AND 
                      strat_data.date=%(date)S AND 
                      strat_data.asset_id=%(asset_id)S
                 """

        with self.__con.cursor() as cursor:
            strategy_id = self._strat_id(name)
            for asset in portfolio.assets:
                asset_id = self._symbol_id(ticker=asset)
                for date in portfolio.index:
                    price = portfolio.prices.loc[date][asset]
                    weight = portfolio.weights.loc[date][asset]
                    data = {"date": date, "asset_id": asset_id, "strategy_id": strategy_id, "price": price,
                            "weight": weight}
                    cursor.execute(upsert, data)



        #x = x.unstack(level=["ts_name", "bloomberg_symbol"])


    def upsert_timeseries(self, ts, asset, field):
        """ Update a timeseries in the database """
        if len(ts) > 0:
            asset_id = self._symbol_id(ticker=asset)
            insert = """
                INSERT INTO ts_name (asset_id, name) VALUES
                (%(asset_id)s::INTEGER, %(field)s)
                ON CONFLICT ON CONSTRAINT name_asset DO NOTHING
            """

            with self.__con.cursor() as cursor:
                cursor.execute(insert, {"asset_id": asset_id, "field": field})
                self.__con.commit()

                ts_id = self._ts_id(ticker=asset, field=field)

                for date, value in ts.items():
                    command = "INSERT INTO ts_data (date, value, ts_id) VALUES ('{date}', {value}, {ts_id}) " \
                              "ON CONFLICT ON CONSTRAINT ts_date_unique " \
                              "DO UPDATE SET value={value} WHERE ts_data.ts_id={ts_id} AND ts_data.date='{date}'".format(date=date, value=value, ts_id=ts_id)
                    cursor.execute(command)
                    self.__con.commit()


    # def upsert_reference(self, frame):
    #     with self.__con.cursor() as cursor:
    #         for ticker, row in frame.iterrows():
    #             field_id = self._field_id(field=row["field"])
    #             asset_id = self._symbol_id(ticker=ticker)
    #             content = row["content"]
    #
    #             x = "INSERT INTO symbolsapp_reference_data (field_id, symbol_id, content) " \
    #                 "VALUES ({field_id}, {symbol_id}, '{content}') " \
    #                 "ON CONFLICT ON CONSTRAINT reference_data_unique " \
    #                 "DO UPDATE SET content='{content}' WHERE " \
    #                 "symbolsapp_reference_data.field_id={field_id} and symbolsapp_reference_data.symbol_id={symbol_id}".format(
    #                 field_id=field_id, symbol_id=asset_id, content=content)
    #
    #             cursor.execute(x)
    #         self.__con.commit()

    @property
    def connection(self):
        return self.__con

    def query(self, query, index_col):
        return pd.read_sql(sql=query, con=self.connection, index_col=index_col)