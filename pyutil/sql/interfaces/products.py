class Reference(object):
    def __init__(self, name, collection):
        self.__collection = collection
        self.__name = name

    @property
    def collection(self):
        return self.__collection

    def __iter__(self):
        for a in self.collection.find(name=self.__name):
            yield a.meta["key"]

    def items(self):
        for a in self.collection.find(name=self.__name):
            yield a.meta["key"], a.data

    def keys(self):
        return set([a for a in self])

    def __setitem__(self, key, value):
        self.collection.upsert(name=self.__name, value=value, key=key)

    def __getitem__(self, item):
        return self.get(item=item, default=None)

    def get(self, item, default=None):
        try:
            return self.collection.find_one(name=self.__name, key=item).data
        except AttributeError:
            return default


class Timeseries(object):
    def __init__(self, name, collection):
        self.__collection = collection
        self.__name = name

    @property
    def collection(self):
        return self.__collection

    def __iter__(self):
        for a in self.collection.find(name=self.__name):
            yield a.meta

    def items(self, **kwargs):
        for a in self.collection.find(name=self.__name, **kwargs):
            yield a.meta, a.data

    def keys(self, **kwargs):
        for a in self.collection.find(name=self.__name, **kwargs):
            yield a.meta

    def read(self, key, **kwargs):
        return self.collection.read(name=self.__name, key=key, **kwargs)

    def write(self, data, key, **kwargs):
        self.collection.write(data=data, key=key, name=self.__name, **kwargs)

    def merge(self, data, key, **kwargs):
        self.collection.merge(data=data, key=key, name=self.__name, **kwargs)

    def last(self, key, **kwargs):
        return self.collection.last(key=key, name=self.__name, **kwargs)

    def __getitem__(self, item):
        return self.read(key=item)

    def __setitem__(self, key, value):
        """
        :param key:
        :param value:
        """
        self.write(data=value, key=key)

