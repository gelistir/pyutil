import logging

import pandas as pd

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from pyutil.portfolio.portfolio import Portfolio
from pyutil.sql.io import postgresql


class Database(object):
    def __init__(self, connection=None, logger=None):
        self.__logger = logger or logging.getLogger(__name__)
        self.__con = connection or postgresql()

    def _strat_id(self, strategy_name):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM strategy WHERE name=%(strategy)s;", {"strategy": strategy_name})
            f = cursor.fetchone()
            assert f is not None, "The strategy {strategy_name} does not exist in the database".format(ticker=ticker)
            return f[0]

    def _symbol_id(self, ticker):
        with self.__con.cursor() as cursor:
            cursor.execute("SELECT id FROM assets WHERE bloomberg_symbol=%(ticker)s;", {"ticker": ticker})
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

    def _history_sql(self, assets=None, names=None):
        cond = []
        if names:
            cond.append("ts_name in {names}".format(names=tuple(names)).replace(',)', ')'))

        if assets:
            cond.append("bloomberg_symbol in {assets}".format(assets=tuple(assets)).replace(',)', ')'))

        w = "WHERE {c}".format(c=" AND ".join(cond)) if cond else ""

        x = self.query("SELECT * FROM ts_data_complete {cond}".format(cond=w),
                       index_col=["bloomberg_symbol", "ts_name", "date"])["value"]
        x = x.unstack(level=["ts_name", "bloomberg_symbol"])
        return x

    def _reference_sql(self, names=None, assets=None):
        cond = []
        if names:
            cond.append("name in {names}".format(names=tuple(names)).replace(',)', ')'))

        if assets:
            cond.append("bloomberg_symbol in {assets}".format(assets=tuple(assets)).replace(',)', ')'))

        w = "WHERE {c}".format(c=" AND ".join(cond)) if cond else ""

        x = self.query("SELECT * FROM reference_data {cond}".format(cond=w),
                        index_col=["bloomberg_symbol", "name"])["content"]
        x = x.unstack(level=["name"])
        return x

    def asset(self, name):
        data = self._history_sql(assets=[name])
        data.columns = data.columns.droplevel(level=1)
        xxx = self.query("SELECT * FROM assets WHERE bloomberg_symbol='{ticker}'".format(ticker=name), index_col="bloomberg_symbol").loc[name]
        ref = self.query("SELECT name, content FROM reference_data WHERE bloomberg_symbol='{ticker}'".format(ticker=name), index_col="name")
        return Asset(name=name, data=data, group=xxx["group_name"], internal=xxx["internal"], link=xxx["webpage"], **ref["content"].to_dict())

    @property
    def reference(self):
        return self._reference_sql()

    def history(self, name="PX_LAST", assets=None):
        frame = self._history_sql(names=[name], assets=assets)
        if not frame.empty:
            return frame[name]
        else:
            return frame

    def assets(self, names=None):
        if names:
            return Assets({name: self.asset(name) for name in names})
        else:
            return Assets({name: self.asset(name) for name in list(self.ticker.index)})

    @property
    def strategies(self):
        return self.query("SELECT * FROM strategy", index_col="name")

    @property
    def ticker(self):
        return self.query("SELECT * FROM assets", index_col="bloomberg_symbol")

    def portfolio(self, name):
        index = ["date", "bloomberg_symbol"]
        f = lambda x: "SELECT date, bloomberg_symbol, {field} FROM strategy_data WHERE name='{strategy}'".format(
            strategy=name, field=x)

        price = self.query(f("price"), index_col=index).unstack(level=1)["price"]
        weight = self.query(f("weight"), index_col=index).unstack(level=1)["weight"]
        return Portfolio(prices=price, weights=weight)

    def timeseries(self, asset, name):
        return self.query("SELECT date, value FROM ts_data_complete WHERE bloomberg_symbol='{asset}' "
                          "AND ts_name='{name}'".format(asset=asset, name=name), index_col=["date"])["value"]

    @property
    def connection(self):
        return self.__con

    def query(self, query, index_col=None):
        self.__logger.debug("Query {query}".format(query=query))
        return pd.read_sql(sql=query, con=self.connection, index_col=index_col)

    def __str__(self):
        return str(self.__con)