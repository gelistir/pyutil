class ReadDict(dict):
    def __init__(self, seq=None, default=None, **kwargs):
        super().__init__(seq, **kwargs)
        self.__default = default

    def __getitem__(self, item):
        return self.get(item, self.__default)

    def _immutable(self, *args, **kws):
        raise TypeError('object is immutable')

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable
    remove = _immutable


class ReadList(list):
    def __init__(self, seq, cls):
        for x in seq:
            assert isinstance(x, cls), "The object {object} has to be of type {type}".format(object=x, type=cls)


        super().__init__(seq)

    def _immutable(self, *args, **kws):
        raise TypeError('object is immutable')

    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    __add__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    __setitem__ = _immutable