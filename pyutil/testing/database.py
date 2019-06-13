import collections
import random
import string
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, scoped_session

# from pyutil.testing.aux import postgresql_db_test
Database = collections.namedtuple("Database", ["session", "connection"])


def __connection_str(user, password, host, database):
    return 'postgresql+psycopg2://{user}:{password}@{host}/{db}'.format(user=user, password=password,
                                                                        host=host, db=database)


def __postgresql_db_test(base, name=None, echo=False):
    # session object
    awake = False
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))

    str_test = __connection_str(user="postgres", password="test", host="test-postgresql", database="postgres")
    str_name = __connection_str(user="postgres", password="test", host="test-postgresql", database=name)

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


def database(base):
    session, connection_str = __postgresql_db_test(base)
    return Database(session=session, connection=connection_str)
