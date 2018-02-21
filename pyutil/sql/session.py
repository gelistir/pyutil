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

def get(session, cls, filter):
    return session.query(cls).filter(*filter).first()

def session(server=None, db=None, user=None, password=None):
    user = user or os.environ["user"]
    password = password or os.environ["password"]
    server = server or os.environ["server"]
    db = db or os.environ["db"]

    engine = create_engine('postgresql+psycopg2://{user}:{password}@{server}/{db}'.format(user=user, password=password, server=server, db=db))
    return sessionmaker(bind=engine)()


def test_session(Base, file=None):
    #with TestEnv() as env:
    if file:
        engine = create_engine("sqlite:///{file}".format(file))
    else:
        engine = create_engine("sqlite://")

    # make the tables...
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    return sessionmaker(bind=engine)()



#
# def upsert(session, cls, get, set=None):
#     """
#     Interacting with Pony entities.
#
#     :param cls: The actual entity class
#     :param get: Identify the object (e.g. row) with this dictionary
#     :param set:
#     :return:
#     """
#     # does the object exist
#     #assert isinstance(cls, EntityMeta), "{cls} is not a database entity".format(cls=cls)
#
#     # if no set dictionary has been specified
#     set = set or {}
#
#     if not session.query(cls).filter(*get).first():
#         cls(*get, *set)
#     if not cls.exists(**get):
#         # make new object
#         return cls(**set, **get)
#     else:
#         # get the existing object
#         obj = cls.get(**get)
#         for key, value in set.items():
#             obj.__setattr__(key, value)
#         return obj
#
# if __name__ == '__main__':
#
#     ll = ["A","B"]
#     print(ll)
#     print(*ll)

