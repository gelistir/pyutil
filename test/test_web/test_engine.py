import json
from unittest import TestCase

from pyutil.web.engine import month, performance
from test.config import series2arrays, read_frame


class Request(object):
    def __init__(self, data):
        self.__data = data.encode('utf-8')

    @property
    def data(self):
        return self.__data


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        ts = read_frame("price.csv")["A"].dropna()
        cls.request = Request(data=json.dumps(series2arrays(ts)))

    def test_month(self):
        x = month(self.request)
        print(x)

    def test_performance(self):
        x = performance(self.request)
        print(x)



