from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.common import DataType
from pyutil.sql.container import Assets, Portfolios
from pyutil.sql.models import PortfolioSQL, Symbol, SymbolType
from pyutil.sql.products import Field
from test.config import test_portfolio


class TestModels(TestCase):
    def testPortfolios(self):
        portfolio = test_portfolio()
        p = PortfolioSQL(name="test")
        p.upsert(portfolio=portfolio)
        x = Portfolios([p])
        print(x.mtd)
        print(x.ytd)


    def testAssets(self):
        s1 = Symbol(bloomberg_symbol="A", internal="a", group=SymbolType.fixed_income)
        s2 = Symbol(bloomberg_symbol="B", internal="b", group=SymbolType.equities)

        f = Field(name="Beauty", result=DataType.integer)
        s1.upsert_ref(field=f, value="10")
        s2.upsert_ref(field=f, value="9")

        a = Assets([s1,s2])

        self.assertDictEqual(a.internal, {'A': 'a', 'B': 'b'})
        self.assertDictEqual(a.group, {'A': 'fixed_income', 'B': 'equities'})

        pdt.assert_frame_equal(a.reference, pd.DataFrame(index=["A","B"], columns=["Beauty"], data=[[10],[9]]))

        for asset in a:
            self.assertIsInstance(asset, Symbol)

