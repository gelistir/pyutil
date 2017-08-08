import os
import sqlite3
import psycopg2
import pandas as pd


def postgresql(conn_str=None):
    conn_str = conn_str or os.environ["POSTGRES"]
    return psycopg2.connect(conn_str)


def sqlite(conn_str=None):
    conn_str = conn_str or os.environ["SQLITE"]
    return sqlite3.connect(conn_str)


def read_table_sql(table, con, index_col=None):
    with con as connection:
        f = pd.read_sql('select * from {table}'.format(table=table), con=connection)

        if index_col:
            f = f.set_index(keys=index_col)

        return f