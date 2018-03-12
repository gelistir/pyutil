from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, Field, PortfolioSQL, Strategy, _SymbolReference, FieldType, \
    Base, SymbolType
from pyutil.sql.session import session_test
from test.config import read_frame, test_portfolio, resource


class TestModels(TestCase):
    def test_type_3(self):
        session = session_test(meta=Base.metadata, file="test4.db")

        f = Field(name="A", type=FieldType.dynamic)
        s = Symbol(bloomberg_symbol="Asset", group=SymbolType.equities)

        x = s.update_reference(field=f, value="28A")
        x = s.update_reference(field=f, value="29A")

        print(x)
        print(x.symbol)
        print(x.field)

        # query the symbol
        print(s.fields)
        # query the field
        print(f.symbols)

        session.add(f)
        session.add(s)
        session.commit()
        #assert False

        # todo: update tests...
