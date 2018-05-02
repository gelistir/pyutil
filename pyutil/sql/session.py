from contextlib import contextmanager

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


@contextmanager
def session_scope(server=None, db=None, user=None, password=None, echo=False):
    """Provide a transactional scope around a series of operations."""
    ses = session(server=server, db=db, user=user, password=password, echo=echo)
    try:
        yield ses
        ses.commit()
    except:
        ses.rollback()
        raise
    finally:
        ses.close()


def session(server=None, db=None, user=None, password=None, echo=False):
    user = user or os.environ["user"]
    password = password or os.environ["password"]
    server = server or os.environ["server"]
    db = db or os.environ["db"]

    engine = create_engine('postgresql+psycopg2://{user}:{password}@{server}/{db}'.format(user=user, password=password, server=server, db=db), echo=echo)
    return sessionmaker(bind=engine)()


def session_test(meta, file=None, echo=False, views=None):
    if file:
        engine = create_engine("sqlite:///{file}".format(file=file), echo=echo)
    else:
        engine = create_engine("sqlite://", echo=echo)

    # make the tables...
    meta.drop_all(engine)
    meta.create_all(engine)

    connection = engine.connect()
    views = views or []

    for a in views:
        connection.execute(a)

    return sessionmaker(bind=engine)()


def session_file(file, echo=False):
    engine = create_engine("sqlite:///{file}".format(file=file), echo=echo)
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



