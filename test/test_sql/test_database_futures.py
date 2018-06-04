from unittest import TestCase

import pandas as pd

from pyutil.sql.base import Base
from pyutil.sql.db_futures import Database
from pyutil.sql.interfaces.futures.future import Future, FuturesCategory, Exchange

from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import session_test

import pandas.util.testing as pdt


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestDatabaseFutures(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_test(meta=Base.metadata, echo=False)
        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)

        cls.fut1 = future()
        cls.fut1.reference[cls.f1] = "100"

        cls.session.add_all([cls.fut1])
        cls.db = Database(session=cls.session)

    def test_futures(self):
        pdt.assert_frame_equal(self.db.futures.reference,
                               pd.DataFrame(index=["ES1 Index"], columns=["Field A"], data=[[100]]), check_names=False)
