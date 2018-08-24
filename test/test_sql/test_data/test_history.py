from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.data.history_interface import HistoryInterface
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test

from test.config import test_portfolio


class LocalHistory(HistoryInterface):
    def __init__(self, session):
        super().__init__(session)

    @staticmethod
    def read_history(ticker, t0, field):
        return test_portfolio().prices[ticker]


class LocalHistoryFaulty(HistoryInterface):
    def __init__(self, session):
        super().__init__(session)

    @staticmethod
    def read_history(ticker, t0, field):
        # simulate a problem on the server
        raise AssertionError



class TestHistory(TestCase):
    @classmethod
    def setUpClass(cls):
        # get a fresh new InfluxDB database
        ProductInterface.client.recreate(dbname="test")

        # create a session to a proper database
        cls.session, connection_str = postgresql_db_test(base=Base)

        # we need to add symbols to the database
        for asset in test_portfolio().prices.keys():
            s = Symbol(name=asset)
            cls.session.add(s)

        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_history(self):
        # this is the data LocalHistory will pump into the database!
        p = test_portfolio().prices

        for symbol in self.session.query(Symbol):
            pdt.assert_series_equal(symbol.price(field="PX_LAST"), pd.Series({}))

        # run will fire off the reading
        hist = LocalHistory(session=self.session)

        pdt.assert_series_equal(hist.age(today=pd.Timestamp("2016-02-21")).dropna(), pd.Series({}, dtype=object))

        hist.run()

        x = hist.age(today=pd.Timestamp("2016-02-21"))
        self.assertEqual(x["A"], 305)

        for symbol in self.session.query(Symbol):
            pdt.assert_series_equal(symbol.price(), p[symbol.name], check_names=False)

        hist.frame(name="History")

        # ask the session for the Frame object...
        x = self.session.query(Frame).filter_by(name="History").one()

        pdt.assert_frame_equal(x.frame, p, check_names=False)


class TestHistoryFaulty(TestCase):
    @classmethod
    def setUpClass(cls):
        # get a fresh new InfluxDB database
        ProductInterface.client.recreate(dbname="test")

        # create a session to a proper database
        cls.session, connection_str = postgresql_db_test(base=Base)

        s = Symbol(name="Maffay")
        cls.session.add(s)
        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_history_faulty(self):
        hist = LocalHistoryFaulty(session=self.session)
        hist.run()

