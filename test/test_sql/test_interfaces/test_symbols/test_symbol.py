import numpy as np
import pandas as pd
import pandas.util.testing as pdt
from unittest import TestCase

from pyutil.influx.client_test import init_influxdb
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t0 = pd.Timestamp("2000-11-17")
t1 = pd.Timestamp("2000-11-18")
t2 = pd.Timestamp("2000-11-19")

series = pd.Series({t0: 100.0, t1: 100.5, t2: np.nan}, name="PX_LAST")


class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        init_influxdb()

    @classmethod
    def tearDownClass(cls):
        Symbol.client.close()

    def test_init(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        self.assertEqual(s.internal, "Peter Maffay")
        self.assertEqual(s.group, SymbolType.equities)
        self.assertEqual(s.discriminator, "symbol")

    def test_ts(self):
        s = Symbol(name="AAAAA US Equity", group=SymbolType.equities, internal="Peter Maffay")
        # update with a series containing a NaN
        s._ts_upsert(ts=series, measurement=Symbol.measurements, field="PX_LAST")
        # extract the series again
        pdt.assert_series_equal(s._ts(measurement=Symbol.measurements, field="PX_LAST"), series.dropna())
        # extract the last stamp
        self.assertEqual(s._last(field="PX_LAST", measurement=Symbol.measurements), t1)
