from unittest import TestCase

import pandas as pd
import numpy as np
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t0 = pd.Timestamp("2000-11-17")
t1 = pd.Timestamp("2000-11-18")
t2 = pd.Timestamp("2000-11-19")

series = pd.Series({t0: 100.0, t1: 100.5, t2: np.nan}, name="px_last")


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
        pdt.assert_series_equal(s.ts(client=self.client, field="px_last"), series.dropna())
        self.assertEqual(s.last(client=self.client), t1)


class TestSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test-symbol2")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="test-symbol2")
        cls.client.close()

    def test_double(self):
        # construct two symbols
        s1 = Symbol(name="A US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s2 = Symbol(name="B US Equity", group=SymbolType.equities, internal="Falco")

        s1.ts_upsert(client=self.client, field="px_last", ts=series)
        s2.ts_upsert(client=self.client, field="px_last", ts=series)

        # but also check the frame...
        print(Symbol.read_frame(client=self.client, field="px_last"))

        #print(Symbol.reference(session=self.session)
