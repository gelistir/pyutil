import os
import random
import string
from time import sleep

import pandas as pd
from functools import partial

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, scoped_session

import collections
Database = collections.namedtuple("Database", ["session", "connection"])

def post(client, data, url):
    response = client.post(url, data=data)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


def get(client, url):
    response = client.get(url)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


def resource_folder(folder):
    def __resource(name, folder):
        return os.path.join(folder, "resources", name)

    return partial(__resource, folder=folder)


# def read_frame(file, header=0, parse_dates=True, index_col=0, **kwargs):
#     return pd.read_csv(file, index_col=index_col, header=header, parse_dates=parse_dates, **kwargs)

#
# def read_series(file, parse_dates=True, index_col=0, cname=None, **kwargs):
#     return pd.read_csv(file, index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates,
#                        names=[cname], **kwargs)

def connection_str(user, password, host, database):
    return 'postgresql+psycopg2://{user}:{password}@{host}/{db}'.format(user=user, password=password,
                                                                        host=host, db=database)

def postgresql_db_test(base, name=None, echo=False):
    # session object
    awake = False
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))

    str_test = connection_str(user="postgres", password="test", host="test-postgresql", database="postgres")
    str_name = connection_str(user="postgres", password="test", host="test-postgresql", database=name)

    while not awake:
        try:
            engine = create_engine(str_test)
            conn = engine.connect()
            conn.execute("commit")
            awake = True
        except OperationalError:
            sleep(1)

    # String interpolation here!? Please avoid
    conn.execute("""DROP DATABASE IF EXISTS {name}""".format(name=name))
    conn.execute("commit")

    conn.execute("""CREATE DATABASE {name}""".format(name=name))
    conn.close()

    engine = create_engine(str_name, echo=echo)

    # drop all tables (even if there are none)
    base.metadata.drop_all(engine)

    # create some tables
    base.metadata.create_all(engine)

    return Database(session=scoped_session(sessionmaker(bind=engine)), connection=str_name)
