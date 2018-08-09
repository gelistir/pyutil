from unittest import TestCase

from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio

import pandas.util.testing as pdt


class _Strategy(ConfigMaster):
    def __init__(self, names, **kwargs):
        super().__init__(names, **kwargs)

    @property
    def portfolio(self):
        return test_portfolio().subportfolio(assets=self.names)


class TestConfigMaster(TestCase):
    def test_run_strategy(self):
        p = test_portfolio()
        s = _Strategy(names=["A","B","C"], reader=lambda name,field=None: p.prices[name])
        pdt.assert_frame_equal(s.history(), p.prices[["A","B","C"]])
        self.assertListEqual(s.names, ["A", "B", "C"])
        self.assertIsNotNone(s.reader)
        pdt.assert_series_equal(s.reader("A"), p.prices["A"])

