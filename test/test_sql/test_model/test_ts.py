from unittest import TestCase

import pandas as pd
from sqlalchemy.exc import IntegrityError

from pyutil.sql.base import Base
from test.test_sql.test_model.ts import Timeseries
from pyutil.sql.session import postgresql_db_test, get_one_or_create
from test.config import test_portfolio


class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, connection_str = postgresql_db_test(base=Base, echo=False)

    def test_same_timeseries_twice(self):
        ts_a = Timeseries(name="price", field="price", measurement="b")
        ts_b = Timeseries(name="price", field="price", measurement="b")
        self.session.add_all([ts_a, ts_b])
        # this should result in an error:

        with self.assertRaises(IntegrityError):
            self.session.commit()

        self.session.rollback()

    def test_x(self):
        ts1 = Timeseries(name="price", field="price", measurement="a")
        ts2 = Timeseries(name="price", field="price", measurement="b", partner="BBB", hans="dampf")
        ts3 = Timeseries(name="haha", field="correlation", measurement="b", partner="CCC")

        self.session.add_all([ts1, ts2, ts3])
        self.session.commit()

        for t in self.session.query(Timeseries).all():
            print({"partner": "CCC"}.items() <= t.keywords.items())
            print(t.keywords)

        for t in self.session.query(Timeseries).filter(Timeseries.name=="haha"):
            print(t)

        for t in self.session.query(Timeseries).filter(Timeseries.field == "price"):
            print(t)

        for t in self.session.query(Timeseries).filter(Timeseries.measurement == "b"):
            print(t)

    def test_create(self):
        x, exists = get_one_or_create(session=self.session, model=Timeseries, name="Peter", field="Maffay", measurement="xxx")
        self.assertFalse(exists)

        self.assertEqual(x.name, "Peter")
        self.assertEqual(x.field, "Maffay")
        self.assertEqual(x.measurement, "xxx")

        y, exists = get_one_or_create(session=self.session, model=Timeseries, name="Peter", field="Maffay",
                                      measurement="xxx")

        self.assertTrue(exists)
        self.assertEqual(y.name, "Peter")
        self.assertEqual(y.field, "Maffay")
        self.assertEqual(y.measurement, "xxx")

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        ts1 = Timeseries(field="nav", measurement="nav", name="test-a")
        ts1.upsert(ts=nav)

        ts2 = Timeseries(field="nav", measurement="nav", name="test-b")
        ts2.upsert(ts=nav)

        self.session.add_all([ts1, ts2])

        def xxx(ts):
            return pd.DataFrame({(t.name, t.field, t.measurement) : t.series for t in ts})

        a = xxx(self.session.query(Timeseries).filter(Timeseries.field=="nav", Timeseries.measurement=="nav").all())
        print(a)
