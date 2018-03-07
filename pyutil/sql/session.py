from contextlib import contextmanager

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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


class SessionDB(object):
    def __init__(self, session):
        super().__init__()
        self.__session = session

    @property
    def session(self):
        return self.__session

    def dictionary(self, cls, key="name"):
        return {obj.__getattribute__(key): obj for obj in self.__session.query(cls)}

    def upsert_one(self, cls, get, set=None):

        s = self.__session.query(cls)
        # apply all filters
        for key, value in get.items():
            s = s.filter(cls.__dict__[key] == value)

        set = set or {}

        if not s.first():
            x = cls(**{**get, **set})
            self.__session.add(x)

        else:
            x = s.first()
            for key, value in set.items():
                x.__setattr__(key, value)

        return x


    def get(self, cls, data):
        s = self.__session.query(cls)
        for key, value in data.items():
            s = s.filter(cls.__dict__[key] == value)

        return s.first()

