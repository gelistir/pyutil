from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import test_client
from pyutil.sql.base import Base
from pyutil.sql.data.reference_interface import ReferenceInterface
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field, FieldType, DataType
from pyutil.sql.session import postgresql_db_test
from test.config import read_frame


class LocalReference(ReferenceInterface):
    @staticmethod
    def read_reference(tickers, fields):
        frame = read_frame("reference.csv", index_col=None)
        for key,row in frame.iterrows():
            yield row["ticker"], row["field"], row["value"]

    def __init__(self, session):
        super().__init__(session)


class TestReference(TestCase):
    @classmethod
    def setUpClass(cls):
        # get a fresh new InfluxDB database
        ProductInterface.client = test_client() #recreate(dbname="test")

        # create a session to a proper database
        cls.session, connection_str = postgresql_db_test(base=Base)

        # we need to add symbols to the database
        for asset in ["A", "B", "C"]:
            s = Symbol(name=asset)
            cls.session.add(s)

        # we need some field for the database
        f1 = Field(name="CHG_PCT_1D", type=FieldType.dynamic, result=DataType.float)
        f2 = Field(name="CRNCY", type=FieldType.static, result=DataType.string)
        f3 = Field(name="PX_CLOSE_DT", type=FieldType.static, result=DataType.date)

        # add those fields
        cls.session.add_all([f1,f2,f3])

        # commit everything
        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_reference(self):
        # check that there is nothing in the database
        for symbol in self.session.query(Symbol):
            pdt.assert_series_equal(symbol.reference_series, pd.Series({}))

        # extract the data and store in database
        ref = LocalReference(session=self.session)
        ref.run()

        # check that the data made it into the database
        #for name, reference in Symbol.reference_frame(products=self.session.query(Symbol)):
        #    print(name)
        #    print(reference)

        frame = Symbol.reference_frame(products=self.session.query(Symbol))
        #frame = pd.concat({name: pd.Series(reference) for name, reference in Symbol.reference_frame(products=self.session.query(Symbol))}).unstack()
        #print(frame)



        self.assertTrue(pd.isna(frame["CHG_PCT_1D"]["C"]))
        self.assertEqual(frame["CHG_PCT_1D"]["A"], 0.1)
        self.assertEqual(frame["CRNCY"]["A"],"EUR")
        self.assertEqual(frame["PX_CLOSE_DT"]["C"], pd.Timestamp("2018-08-08").date())

        ref.frame(name="Reference")

        # ask the session for the Frame object...
        x = self.session.query(Frame).filter_by(name="Reference").one()

        frame = x.frame

        self.assertTrue(pd.isna(frame["CHG_PCT_1D"]["C"]))
        self.assertEqual(frame["CHG_PCT_1D"]["A"], 0.1)
        self.assertEqual(frame["CRNCY"]["A"],"EUR")
