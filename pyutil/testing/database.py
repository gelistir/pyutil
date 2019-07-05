import collections
import random
import string

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils.functions import database_exists, create_database, drop_database

Database = collections.namedtuple("Database", ["session", "connection"])


def __connection_str(user, password, host, database):
    return 'postgresql+psycopg2://{user}:{password}@{host}/{db}'.format(user=user, password=password,
                                                                        host=host, db=database)


def __postgresql_db_test(base, name=None, echo=False):
    # use the name or create a random name
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    # create a connection_str
    str_name = __connection_str(user="postgres", password="test", host="test-postgresql", database=name)

    # create an engine
    engine = create_engine(str_name, echo=echo)

    # if the database exists already, drop it
    if database_exists(str_name):
        drop_database(engine.url)

    # create a database
    create_database(engine.url)

    # create the tables
    base.metadata.create_all(engine)

    return Database(session=scoped_session(sessionmaker(bind=engine)), connection=str_name)


def database(base, name=None, echo=False):
    session, connection_str = __postgresql_db_test(base, name=name, echo=echo)
    return Database(session=session, connection=connection_str)
