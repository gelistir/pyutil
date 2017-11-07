
from pony.orm.core import EntityMeta
from pony import orm


def upsert(cls, get, set=None):
    """
    Interacting with Pony entities.

    :param cls: The actual entity class
    :param get:
    :param set:
    :return:
    """
    # does the object exist
    assert isinstance(cls, EntityMeta), "{cls} is not a database entity".format(cls=cls)

    # if no set dictionary has been specified
    set = set or {}

    if not cls.exists(**get):
        # make new object
        return cls(**set, **get)
    else:
        # get the existing object
        obj = cls.get(**get)
        for key, value in set.items():
            obj.__setattr__(key, value)
        return obj


def db_in_memory(db):
    # if the database object is already bound to a db (will raise a TypeError?!)
    # if so, go on, clear all data and recreate the tables
    try:
        db.bind(provider='sqlite', filename=":memory:")
        db.generate_mapping(create_tables=True)
    except TypeError:
        pass

    db.drop_all_tables(with_all_data=True)
    db.create_tables()
    return orm.db_session()
