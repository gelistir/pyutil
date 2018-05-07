from unittest import TestCase

import pandas as pd


from pyutil.sql.base import Base
from pyutil.sql.db_symbols import Database
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import session_test

import pandas.util.testing as pdt


class TestDatabaseSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_test(meta=Base.metadata, echo=False)

        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)
        cls.s1 = Symbol(name="AA", group=SymbolType.equities)

        cls.s1.reference[cls.f1] = "100"

        cls.p1 = Portfolio(name="Peter")
        cls.p1.reference[cls.f1] = "200"

        cls.session.add_all([cls.s1, cls.p1])
        cls.db = Database(session=cls.session)

    def test_symbols(self):
        pdt.assert_frame_equal(self.db.symbols.reference, pd.DataFrame(index=["AA"], columns=["Field A"], data=[[100]]), check_names=False)

    def test_portfolios(self):
        pdt.assert_frame_equal(self.db.portfolios.reference, pd.DataFrame(index=["Peter"], columns=["Field A"], data=[[200]]), check_names=False)
