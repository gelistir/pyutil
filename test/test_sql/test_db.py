from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.db import db, Timeseries, Symbol, TimeseriesData, SymbolGroup, Type, Field, SymbolReference
from test.config import read_frame
from pyutil.sql.pony import db_in_memory



class TestHistory(TestCase):
    def test_series(self):
        with db_in_memory(db):
            prices = read_frame("price.csv")
            for symbol in ["A", "B", "C", "D"]:
                ts = Timeseries(asset=Symbol(bloomberg_symbol=symbol, group=SymbolGroup(name=symbol)), name="PX_LAST")
                for date, value in prices[symbol].dropna().items():
                    TimeseriesData(ts=ts, date=date, value=value)

            t = Symbol.get(bloomberg_symbol="A").series["PX_LAST"]
            pdt.assert_series_equal(t, read_frame("price.csv")["A"].dropna(), check_names=False)

            tt = Timeseries.get(asset=Symbol.get(bloomberg_symbol="A"), name="PX_LAST")
            self.assertFalse(tt.empty)
            self.assertEqual(tt.last_valid_index, pd.Timestamp("2015-04-22").date())


    def test_ref(self):
        with db_in_memory(db):
            t1 = Type(name="BB-static", comment="Static data Bloomberg")
            t2 = Type(name="BB-dynamic", comment="Dynamic data Bloomberg")
            t3 = Type(name="user-defined", comment="User-defined reference data")
            Field(name="CHG_PCT_1D", type=t2)
            Field(name="NAME", type=t1)
            Field(name="REGION", type=t3)

            Symbol(bloomberg_symbol="XX", group=SymbolGroup(name="A"))
            Symbol(bloomberg_symbol="YY", group=SymbolGroup(name="B"))

            SymbolReference(field=Field.get(name="CHG_PCT_1D"), symbol=Symbol.get(bloomberg_symbol="XX"), content="0.40")
            SymbolReference(field=Field.get(name="NAME"), symbol=Symbol.get(bloomberg_symbol="XX"), content="Hans")
            SymbolReference(field=Field.get(name="REGION"), symbol=Symbol.get(bloomberg_symbol="XX"), content="Europe")

            SymbolReference(field=Field.get(name="CHG_PCT_1D"), symbol=Symbol.get(bloomberg_symbol="YY"), content="0.40")
            SymbolReference(field=Field.get(name="NAME"), symbol=Symbol.get(bloomberg_symbol="YY"), content="Urs")
            SymbolReference(field=Field.get(name="REGION"), symbol=Symbol.get(bloomberg_symbol="YY"), content="Europe")


            s = Symbol.get(bloomberg_symbol="XX")
            self.assertEqual(s.reference["REGION"], "Europe")


            #assert False


            #print(s.reference["CHG_PCT_1D"], "abc")
            #print(s.reference["NAME"], "abc")

            #s = Symbol.get(bloomberg_symbol="YY")
            #print(s.reference["CHG_PCT_1D"], "abc")
            #print(s.reference["NAME"], "abc")