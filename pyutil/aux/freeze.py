def freeze(obj):
    try:
        hash(obj)
        return obj
    except TypeError:
        freezer = getattr(obj, '__freeze__', None)
        if freezer:
            return freezer()
        raise TypeError('object is not freezable')


class xset(set):
    def __freeze__(self):
        return frozenset(self)


class xlist(list):
    def __freeze__(self):
        return tuple(self)


class xdict(dict):
    def __freeze__(self):
        return _imdict(self)


class _imdict(dict):
    def __hash__(self):
        return id(self)

    def _immutable(self, *args, **kws):
        raise TypeError('object is immutable')

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable


if __name__ == '__main__':
    x = xdict({"a": 2.0, "b": 3.0})
    # freeze a dictionary...
    print(freeze({"a": 2.0, "b": 3.0}))
