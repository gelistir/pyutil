from unittest import TestCase

import pandas as pd
import numpy as np
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.model.ref import Field, DataType

t0 = pd.Timestamp("2000-11-17")
t1 = pd.Timestamp("2000-11-18")
t2 = pd.Timestamp("2000-11-19")

series = pd.Series({t0: 100.0, t1: 100.5, t2: np.nan}, name="PX_LAST")


class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test-symbol")
        Symbol.client = cls.client

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
        pdt.assert_series_equal(s.ts(), pd.Series({}))
        self.assertIsNone(s.last())

    def test_ts(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s.ts_upsert(ts=series)
        pdt.assert_series_equal(s.ts(), series.dropna())
        self.assertEqual(s.last(), t1)


class TestSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="test-symbol2")
        Symbol.client = cls.client

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="test-symbol2")
        cls.client.close()

    def test_double(self):
        # construct two symbols
        s1 = Symbol(name="A US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s2 = Symbol(name="B US Equity", group=SymbolType.equities, internal="Falco")

        s1.ts_upsert(ts=series)
        s2.ts_upsert(ts=series)

        # but also check the frame...
        print(Symbol.read_frame(field="PX_LAST"))

        pdt.assert_frame_equal(Symbol.group_internal([s1, s2]),
                               pd.DataFrame(index=["A US Equity", "B US Equity"],
                                            columns=["group", "internal"],
                                            data=[["equities", "Peter Maffay"], ["equities", "Falco"]]))

    def test_reference(self):
        s1 = Symbol(name="A US Equity", group=SymbolType.equities, internal="Peter Maffay")
        s2 = Symbol(name="B US Equity", group=SymbolType.equities, internal="Falco")
        s3 = Symbol(name="C US Equity", group=SymbolType.currency, internal="HAHA")

        f1 = Field(name="A", result=DataType.integer)
        f2 = Field(name="B", result=DataType.integer)
        s1.reference[f1] = "200"
        s3.reference[f2] = "500"

        pdt.assert_frame_equal(Symbol.reference_frame(symbols=[s1, s2]), pd.DataFrame(index=["A US Equity"], columns=["A"], data=[200]), check_names=False)
        pdt.assert_frame_equal(Symbol.reference_frame(symbols=[s1, s3]), pd.DataFrame(index=["A US Equity", "C US Equity"], columns=["A","B"], data=[[200, ""], ["", 500]]), check_names=False)
