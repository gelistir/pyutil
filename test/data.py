import os
import sqlite3

import pandas as pd
from pyutil.mongo.asset import Asset
from pyutil.performance.summary import NavSeries
from pyutil.sql.data import Database
from pyutil.sql.io import postgresql, sqlite
from test.config import read_series, resource

if __name__ == '__main__':
    pd.options.display.width = 300

    s = NavSeries(read_series("ts.csv", parse_dates=True))

    #frame.to_csv(os.path.join("/data", "price_archive.csv"))

    # construct a new set of assets with reduced time series data
    #def asset(name):
    #    return Asset(name, data=frame[name].to_frame(name="PX_LAST"))

    # connect (read-only) to production database
    #db = Database(connection=postgresql(os.environ["POSTGRES_READER"]))

    # connect to a sqlite database for testing purposes
    file = resource("database.db")
    Database(connection=sqlite(conn_str=file))

    #print(db)

    s.summary().to_csv(resource("summary.csv"))
    s.drawdown.to_csv(resource("drawdown.csv"))
    s.monthlytable.to_csv(resource("monthtable.csv"))

    #db.strategies.to_sql(name="strategy", con=sqlite3.connect(file), if_exists="replace")

    #for (strategy, portfolio) in main(sqlite3.connect(file), reader=asset):
    #    portfolio.weights.tail(10).sort_index(axis=1).to_csv(os.path.join("/data", "w{name}.csv".format(name=strategy)))