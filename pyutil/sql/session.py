import random
import string
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


def str_postgres(user="postgres", password="test", server="test-postgresql", db="postgres"):
    return 'postgresql+psycopg2://{user}:{password}@{server}/{db}'.format(user=user, password=password, server=server, db=db)


#def str2session(connection_str, echo=False):
#    engine = create_engine(connection_str, echo=echo)
#    factory = sessionmaker(bind=engine)
#    return factory()


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

    str_test = str_postgres()
    assert str_test == "postgresql+psycopg2://postgres:test@test-postgresql/postgres"

    while not awake:
        try:
            engine = create_engine(str_test)
            conn = engine.connect()
            conn.execute("commit")
            awake = True
        except OperationalError:
            print(sleep)
            sleep(1)

    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    # String interpolation here!? Please avoid
    conn.execute("""DROP DATABASE IF EXISTS {name}""".format(name=name))
    conn.execute("commit")

    conn.execute("""CREATE DATABASE {name}""".format(name=name))
    conn.close()

    engine = create_engine(str_postgres(db=name), echo=echo)

    # drop all tables (even if there are none)
    base.metadata.drop_all(engine)

    # create some tables
    base.metadata.create_all(engine)

    if views:
        with open(views) as file:
            engine.execute(file.read())

    return sessionmaker(bind=engine)()

