import os
import sqlite3
import psycopg2


def postgresql(conn_str=None):
    conn_str = conn_str or os.environ["POSTGRES"]
    return psycopg2.connect(conn_str)


def sqlite(conn_str=None):
    conn_str = conn_str or os.environ["SQLITE"]
    return sqlite3.connect(conn_str)
