from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound


@contextmanager
def session(connection_str, echo=False, base=None):
    """Provide a transactional scope around a series of operations."""
    try:
        engine = create_engine(connection_str, echo=echo)
        # if the user has specified a base
        if base:
            base.metadata.create_all(engine)

        connection = engine.connect()
        s = Session(bind=connection)
        yield s
        s.commit()
    except Exception as e:
        s.rollback()
        raise e
    finally:
        s.close()


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
