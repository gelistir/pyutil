import os
import random
import string
from contextlib import contextmanager
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.interfaces.products import ProductInterface


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def session(server=None, db=None, user=None, password=None, echo=False):
    user = user or os.environ["user"]
    password = password or os.environ["password"]
    server = server or os.environ["server"]
    db = db or os.environ["db"]

    engine = create_engine('postgresql+psycopg2://{user}:{password}@{server}/{db}'.format(user=user, password=password, server=server, db=db), echo=echo)
    return sessionmaker(bind=engine)()


def session_test(meta, echo=False):
    engine = create_engine("sqlite://", echo=echo)

    # make the tables...
    meta.drop_all(engine)
    meta.create_all(engine)

    return sessionmaker(bind=engine)()


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
    # get a fresh new InfluxDB database
    ProductInterface.client.recreate(dbname="test")

    # session object
    awake = False
    while not awake:
        try:
            engine = create_engine("postgresql+psycopg2://postgres:test@test-postgresql/postgres")
            conn = engine.connect()
            conn.execute("commit")
            awake = True
        except OperationalError:
            sleep(1)

    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    # String interpolation here!? Please avoid
    conn.execute("""DROP DATABASE IF EXISTS {name}""".format(name=name))
    conn.execute("commit")

    conn.execute("""CREATE DATABASE {name}""".format(name=name))
    conn.close()

    s = session(server="test-postgresql", password="test", user="postgres", db=name, echo=echo)


    # drop all tables (even if there are none)
    base.metadata.drop_all(s.bind)

    # create some tables
    base.metadata.create_all(s.bind)

    if views:
        with open(views) as file:
            s.bind.execute(file.read())

    return s

