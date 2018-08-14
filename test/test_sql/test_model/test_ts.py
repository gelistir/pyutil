import pandas as pd
from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.model.ts import Timeseries
from pyutil.sql.session import postgresql_db_test
from test.config import test_portfolio

import pandas.util.testing as pdt

class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=False)

        #cls.p1 = Product(name="A")
        #cls.p2 = Product(name="B")
        #cls.p3 = Product(name="C")
        #cls.session.add_all([cls.p1, cls.p2, cls.p3])

    def test_y(self):
        d = dict()
        tags = ["a","b"]
        x1 = {"a": 2.0, "b": 3.0, "d": 4.0, "c": 6.0}
        print(x1["a"])

        t1 = tuple({"a": "aa", "b": "bb"}.values())
        t2 = tuple({"a": "cc", "b": "dd"}.values())

        d[t1] = pd.Series({"xxx": 2.0})
        d[t2] = pd.Series({"xxx": 2.0})

        print(d)
        print(pd.DataFrame(d))
        assert False

    def test_x(self):
        ts1 = Timeseries(field="price", measurement="a")
        ts2 = Timeseries(field="correlation", measurement="b", partner="BBB", hans="dampf")
        ts3 = Timeseries(field="correlation", measurement="b", partner="CCC")

        self.session.add_all([ts1, ts2, ts3])
        self.session.commit()

        #.items()
        #print({"partner": "C"} in ts.keywords)
        #self.session.query(Timeseries).filter({"partner": "C"}.items() <= dict(Timeseries.keywords).items())
        for t in self.session.query(Timeseries).all():
            print({"partner": "CCC"}.items() <= t.xxx.items())
            print(t.xxx)


            #print(t.name)
            #print(t.keywords)


        assert False

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        ts1 = Timeseries(field="nav", measurement="nav", name="test-a", aaa="peter", bbb="wurst")
        ts1.upsert(ts=nav)

        ts2 = Timeseries(field="nav", measurement="nav", name="test-b", aaa="hans", bbb="wurst")
        ts2.upsert(ts=nav)
        #assert False

        self.session.add_all([ts1, ts2])

        #conditions = {"name": "test-b"}
        for x in self.session.query(Timeseries).filter(Timeseries.field=="nav", Timeseries.measurement=="nav").all():
            if {"name": "test-b"}.items() <= x.xxx.items():
                pdt.assert_series_equal(x.series, pd.Series(index=nav.index, data=nav.data))

        tags = ["aaa", "name"]
        d = dict()
        conditions = {}
        for x in self.session.query(Timeseries).filter(Timeseries.field=="nav", Timeseries.measurement=="nav").all():
            if conditions.items() <= x.xxx.items():
                v = tuple({tag : x.xxx[tag] for tag in tags}.values())
                d[v] = x.series

        frame = pd.DataFrame(d)
        #frame = frame.unstack(level=-1)
        print(frame)
        assert False

        y = self.client.read_frame(field="navframe", measurement="nav2", tags=["namex"])
        pdt.assert_series_equal(nav, y["test-a"], check_names=False)
        pdt.assert_series_equal(nav.tail(20), y["test-b"].dropna(), check_names=False)


        #self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")
        #pdt.assert_series_equal(nav, self.client.read_series(field="nav", measurement="nav", conditions={"name": "test-a"}))

        #assert False

        # alternative way to read the series
        #x = self.client.read_series(field="nav", measurement="nav", tags=["name"])
        #pdt.assert_series_equal(nav, x.loc["test-a"].dropna(), check_names=False)
        #assert "nav" in self.client.measurements
