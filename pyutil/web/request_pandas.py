import json
import pandas as pd


class RequestPandas(object):
    def __init__(self, data=None):
        self.__data = data or dict()

    def keys(self):
        return self.__data.keys()

    def __setitem__(self, key, value):
        if isinstance(value, pd.Series):
            self.__data[key] = value.to_json()
            return

        if isinstance(value, pd.DataFrame):
            self.__data[key] = value.to_json(orient="split")
            return

        self.__data[key] = value

    def json(self):
        return json.dumps(self.__data)

    def get_scalar(self, name, default=None):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return self.__data[name] if name in self.__data.keys() else default

    def get_series(self, name):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return pd.read_json(self.__data[name], typ="series")

    def get_frame(self, name):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return pd.read_json(self.__data[name], typ="frame", orient="split")