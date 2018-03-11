from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, SymbolGroup, Field, PortfolioSQL, Strategy, _SymbolReference, MyType
from pyutil.sql.session import session_test
from test.config import read_frame, test_portfolio, resource


class TestModels(TestCase):
    def test_type_3(self):
        f = Field(name="A", _id= 10, type=MyType.dynamic)
        s = Symbol(bloomberg_symbol="Asset", _id=5, group=SymbolGroup(name="Maffay"))

        x = s.update_reference(field=f, value="28A")
        x = s.update_reference(field=f, value="29A")

        print(x)
        print(x.symbol)
        print(x.field)

        # query the symbol
        print(s.reference)
        # query the field
        print(f.data)

        # todo: update tests...
