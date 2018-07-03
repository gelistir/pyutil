from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t0 = pd.Timestamp("2000-11-17")
t1 = pd.Timestamp("2000-11-18")

class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="test")

    def test_init(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        self.assertEqual(s.internal, "Peter Maffay")
        self.assertEqual(s.group, SymbolType.equities)
        self.assertEqual(s.discriminator, "symbol")

    def test_ts(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        pdt.assert_series_equal(s.ts(client=self.client, field="px_last"), pd.Series({}))

        s.ts_upsert(client=self.client, field="px_last", ts={t0: 100.0, t1: 100.5})
        pdt.assert_series_equal(s.ts(client=self.client, field="px_last"), pd.Series({t0.date(): 100.0, t1.date(): 100.5}, name="px_last"))

        self.assertEqual(s.last(client=self.client), t1.date())
