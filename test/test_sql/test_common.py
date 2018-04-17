from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.common import DataType, history, reference
from pyutil.sql.database import Database
from pyutil.sql.models import SymbolType, Symbol
from pyutil.sql.products import Field
from pyutil.sql.session import session_test
from test.config import read_frame


class TestCommon(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_test(meta=Base.metadata)

        s1 = Symbol(bloomberg_symbol="A", group=SymbolType.equities, internal="A")
        s2 = Symbol(bloomberg_symbol="B", group=SymbolType.equities, internal="B")
        s3 = Symbol(bloomberg_symbol="C", group=SymbolType.fixed_income, internal="C")

        # add all symbols to database
        cls.session.add_all([s1, s2, s3])

        f1 = Field(name="Field 1", result=DataType.integer)
        f2 = Field(name="Field 2", result=DataType.integer)

        s1._refdata_proxy[f1] = 100
        s1._refdata_proxy[f2] = 200
        s2._refdata_proxy[f1] = 10
        s2._refdata_proxy[f2] = 20
        s3._refdata_proxy[f1] = 30
        s3._refdata_proxy[f2] = 40

        s1.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["A"])
        s2.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["B"])
        s3.upsert_ts(name="PX_LAST").upsert(ts=read_frame("price.csv")["C"])

        cls.db = Database(session=cls.session)
        cls.session.commit()

    def test_field(self):
        d = DataType.integer
        self.assertEqual(d("100"), 100)
        self.assertEqual(d.value, "integer")

    def test_history(self):
        def f(frame):
            frame.index = [a.date() for a in frame.index]
            return frame

        s1 = self.session.query(Symbol).filter_by(bloomberg_symbol="A").one()
        s2 = self.session.query(Symbol).filter_by(bloomberg_symbol="B").one()
        frame = f(read_frame("price.csv")[["A", "B"]])
        frame = frame.rename(columns={"A": s1, "B": s2})
        frame.index.names = ["Date"]

        pdt.assert_frame_equal(history(products=[s1, s2], field="PX_LAST"), frame)

    def test_reference(self):
        s1 = self.session.query(Symbol).filter_by(bloomberg_symbol="A").one()
        s2 = self.session.query(Symbol).filter_by(bloomberg_symbol="B").one()

        f1 = self.session.query(Field).filter_by(name="Field 1").one()
        f2 = self.session.query(Field).filter_by(name="Field 2").one()

        frame = pd.DataFrame(index=[s1, s2], columns=[f1.name, f2.name], data=[[100, 200], [10, 20]])
        frame.index.names = ["Product"]

        pdt.assert_frame_equal(reference(products=[s1, s2]), frame)
