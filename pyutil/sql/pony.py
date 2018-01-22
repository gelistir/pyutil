from pony.orm.core import EntityMeta


def upsert(cls, get, set=None):
    """
    Interacting with Pony entities.

    :param cls: The actual entity class
    :param get: Identify the object (e.g. row) with this dictionary
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
