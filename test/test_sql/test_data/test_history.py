from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.data.history_interface import HistoryInterface
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test
from test.config import test_portfolio


class LocalHistory(HistoryInterface):
    def __init__(self, session):
        super().__init__(session)
        self.__prices = test_portfolio().prices

    def read(self, ticker, t0, field):
        return self.__prices[ticker]


class TestHistory(TestCase):
    @classmethod
    def setUpClass(cls):
        # create a session to a proper database
        cls.session = postgresql_db_test(base=Base)

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
        LocalHistory(session=self.session).run()

        for symbol in self.session.query(Symbol):
            pdt.assert_series_equal(symbol.price(), p[symbol.name], check_names=False)

