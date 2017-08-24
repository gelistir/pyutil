import collections
from types import MappingProxyType


class ImmutableDict(collections.Mapping):
    """
    Copies a dict and proxies it via types.MappingProxyType to make it immutable.
    """
    def __init__(self, somedict):
        dictcopy = dict(somedict) # make a copy
        self._dict = MappingProxyType(dictcopy) # lock it
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(frozenset(self._dict.items()))
        return self._hash

    def __eq__(self, other):
        return self._dict == other._dict

    def __repr__(self):
        return str(self._dict)



if __name__ == '__main__':
    d = {"a":2 , "b": 3}
    x = ImmutableDict(d)
    print(x)
    x._dict["a"] = 10

