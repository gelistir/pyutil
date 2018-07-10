from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t0 = pd.Timestamp("2000-11-17")
t1 = pd.Timestamp("2000-11-18")

series = pd.Series({t0: 100.0, t1: 100.5}, name="px_last")

class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test-symbol")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="test-symbol")

    def test_init(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        self.assertEqual(s.internal, "Peter Maffay")
        self.assertEqual(s.group, SymbolType.equities)
        self.assertEqual(s.discriminator, "symbol")

    def test_empty_ts(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        pdt.assert_series_equal(s.ts(client=self.client, field="px_last"), pd.Series({}))
        self.assertIsNone(s.last(client=self.client))

    def test_ts(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s.ts_upsert(client=self.client, field="px_last", ts=series)
        pdt.assert_series_equal(s.ts(client=self.client, field="px_last"), series)
        self.assertEqual(s.last(client=self.client), t1)


class TestSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test-symbol2")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="test-symbol2")

    def test_double(self):
        # construct two symbols
        s1 = Symbol(name="A US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s2 = Symbol(name="B US Equity", group=SymbolType.equities, internal="Falco")

        s1.ts_upsert(client=self.client, field="px_last", ts=series)
        s2.ts_upsert(client=self.client, field="px_last", ts=series)

        # construct a frame for two symbols
        #x = pd.DataFrame(index=[pd.Timestamp("2010-01-01")], columns=["A US Equity", "B US Equity"], data=[[10.1, 11.2]])

        # write frame to database
        #Symbol.write_frame(client=self.client, frame=x, name="px_last")

        # check the symbols
        #pdt.assert_series_equal(s1.ts(client=self.client), x["A US Equity"])
        #pdt.assert_series_equal(s2.ts(client=self.client), x["B US Equity"])

        # but also check the frame...
        print(Symbol.read_frame(client=self.client))

        #pdt.assert_frame_equal(Symbol.read_frame(client=self.client), x)

