from contextlib import contextmanager

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    session = session
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def session(server=None, db=None, user=None, password=None):
    user = user or os.environ["user"]
    password = password or os.environ["password"]
    server = server or os.environ["server"]
    db = db or os.environ["db"]

    engine = create_engine('postgresql+psycopg2://{user}:{password}@{server}/{db}'.format(user=user, password=password, server=server, db=db))
    return sessionmaker(bind=engine)()


def session_test(meta, file=None):
    if file:
        engine = create_engine("sqlite:///{file}".format(file=file))
    else:
        engine = create_engine("sqlite://")

    # make the tables...
    meta.drop_all(engine)
    meta.create_all(engine)

    return sessionmaker(bind=engine)()


def session_file(file):
    engine = create_engine("sqlite:///{file}".format(file=file))
    return sessionmaker(bind=engine)()


def get_one_or_create(session, model, **kwargs):
    #  see http://skien.cc/blog/2014/01/15/sqlalchemy-and-race-conditions-implementing/

    try:
        return session.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        # create the model object
        a = model(**kwargs)
        # add it to session
        session.add(a)
        return a

def get_one_or_none(session, model, **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        return None



