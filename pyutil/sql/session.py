import collections
import random
import string
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


def connection_str(user, password, host, database):
    return 'postgresql+psycopg2://{user}:{password}@{host}/{db}'.format(user=user, password=password,
                                                                        host=host, db=database)

def get_one_or_create(session, model, **kwargs):
    #  see http://skien.cc/blog/2014/01/15/sqlalchemy-and-race-conditions-implementing/

    try:
        return session.query(model).filter_by(**kwargs).one(), True
    except NoResultFound:
        # create the test_model object
        a = model(**kwargs)
        # add it to session
        session.add(a)
        return a, False


def get_one_or_none(session, model, **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        return None


def postgresql_db_test(base, name=None, echo=False, views=None):
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

    if views:
        with open(views) as file:
            engine.execute(file.read())

    return sessionmaker(bind=engine)(), str_name
