from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyutil.sql.models import Base, Frame, Symbol, SymbolGroup, Timeseries, TimeseriesData, Type, Field, SymbolReference
from test.config import read_frame


class TestHistory(TestCase):

    @staticmethod
    def session():
        # with TestEnv() as env:
        engine = create_engine("sqlite://")

        # make the tables...
        Base.metadata.create_all(engine)

        return sessionmaker(bind=engine)()

    def test_series(self):
        session = self.session()

        prices = read_frame("price.csv")
        for symbol in ["A", "B", "C", "D"]:
            g = SymbolGroup(name=symbol)
            g.symbols = [Symbol(bloomberg_symbol=symbol, timeseries={"PX_LAST": Timeseries(name="PX_LAST")})]

            #g.symbols[0].timeseries = [Timeseries(name="PX_LAST")]
            g.symbols[0].timeseries["PX_LAST"].data = {date : TimeseriesData(date=date, value=value) for date, value in prices[symbol].dropna().items()}
            # thanks to cascade all of those objects will go into our database
            session.add(g)

        t = session.query(Symbol).filter(Symbol.bloomberg_symbol == "A").first()
        s = t.timeseries["PX_LAST"].series

        pdt.assert_series_equal(s, read_frame("price.csv")["A"].dropna(), check_names=False)

        self.assertFalse(t.timeseries["PX_LAST"].empty)
        self.assertEqual(t.timeseries["PX_LAST"].last_valid.date(), pd.Timestamp("2015-04-22").date())


        t.timeseries["PX_LAST"].upsert(pd.Series(index=[pd.Timestamp("2015-04-22"), pd.Timestamp("2015-04-23")], data=[200, 300]))
        print(t.timeseries["PX_LAST"].series)


    def test_ref(self):
        session = self.session()

        t1 = Type(name="BB-static", fields= [Field(name="Name")])
        t2 = Type(name="BB-dynamic", fields=[Field(name="CHG_PCT_1D")])
        t3 = Type(name="user-defined", fields=[Field(name="REGION")])
        session.add_all([t1,t2,t3])

        g1 = SymbolGroup(name="A", symbols=[Symbol(bloomberg_symbol="XX")])
        g2 = SymbolGroup(name="B", symbols=[Symbol(bloomberg_symbol="YY")])
        session.add_all([g1,g2])

        name = t1.fields[0]
        chg = t2.fields[0]
        region = t3.fields[0]

        xx = g1.symbols[0]
        yy = g2.symbols[0]

        SymbolReference(field=name, symbol=xx, content="Hans")
        SymbolReference(field=region, symbol=xx, content="Europe")
        SymbolReference(field=chg, symbol=xx, content="0.40")

        SymbolReference(field=name, symbol=yy, content="Urs")
        SymbolReference(field=region, symbol=yy, content="Europe")
        SymbolReference(field=chg, symbol=yy, content="0.40")


        s = session.query(Symbol).filter(Symbol.bloomberg_symbol=="XX").first()
        self.assertEqual(s.reference["REGION"], "Europe")

        g1.symbols[0].update_reference(t1.fields[0], "Wurst")
        s = session.query(Symbol).filter(Symbol.bloomberg_symbol=="XX").first()
        self.assertEqual(s.reference["Name"], "Wurst")

    def test_frame(self):
        session = self.session()

        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A","B"], columns=["X1", "X2"])
        x.index.names = ["index"]

        f = Frame(frame=x, name="test")
        session.add(f)
        session.commit()

        xxx = session.query(Frame).filter(Frame.name=="test").first()
        f = xxx.frame

        pdt.assert_frame_equal(f, x)
