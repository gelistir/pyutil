from unittest import TestCase

import pandas as pd
from sqlalchemy.orm.exc import NoResultFound

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

        # create a view on the spot
        #a = cls.session.connection()
        #a.execute("CREATE VIEW my_view AS SELECT * FROM symbol")

        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)
        cls.s1 = Symbol(bloomberg_symbol="AA", group=SymbolType.equities)

        cls.s1.reference[cls.f1] = "100"

        cls.p1 = Portfolio(name="Peter")
        cls.p1.reference[cls.f1] = "200"

        cls.session.add_all([cls.s1, cls.p1])
        cls.db = Database(session=cls.session)

    def test_symbols(self):
        pdt.assert_frame_equal(self.db.symbols.reference,
                               pd.DataFrame(index=[self.s1], columns=["Field A"], data=[[100]]), check_names=False)

    def test_portfolios(self):
        pdt.assert_frame_equal(self.db.portfolios.reference,
                               pd.DataFrame(index=[self.p1], columns=["Field A"], data=[[200]]), check_names=False)

    def test_portfolio(self):
        with self.assertRaises(NoResultFound):
            self.db.portfolio(name="5")

        p = self.db.portfolio(name="Peter")
        self.assertEqual(p.reference[self.f1], 200)

    def test_symbols(self):
        with self.assertRaises(NoResultFound):
            self.db.symbol(bloomberg_symbol="HaHA")

        s = self.db.symbol(bloomberg_symbol="AA")
        self.assertEqual(s.reference[self.f1], 100)

    #def test_view_3(self):
    #    a = pd.read_sql("SELECT * FROM my_view", con=self.session.connection(), index_col=["bloomberg_symbol"])
    #    pdt.assert_frame_equal(a, pd.DataFrame(index=["AA"], columns=["group", "internal", "id"], data=[["equities",None,1]]), check_names=False)