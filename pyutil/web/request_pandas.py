import json
import pandas as pd


class RequestPandas(object):
    # data is a json document
    def __init__(self, json_str=None):
        if json_str:
            if isinstance(json_str, str):
                # weird construction, but if data is None, you can not apply json.loads
                self.__data = json.loads(json_str)
            else:
                self.__data = json.loads(json_str.data.decode("utf-8"))

        else:
            self.__data = dict()

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
        return json.dumps(self.__data, sort_keys=True)

    def get(self, name, default=None):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return self.__data[name] if name in self.__data.keys() else default

    def get_series(self, name):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return pd.read_json(self.__data[name], typ="series")

    def get_frame(self, name):
        assert name in self.__data.keys(), "The key {0} is unknown".format(name)
        return pd.read_json(self.__data[name], typ="frame", orient="split")