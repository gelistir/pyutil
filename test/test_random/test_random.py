from unittest import TestCase
import numpy as np

from pyutil.random.rand import rand_asset, rand_assets


class TestRandom(TestCase):

    def setUp(self):
        np.random.seed(seed=0)

    def test_asset(self):
        x = rand_asset(t0="2016-01-01", t1="2017-01-01")
        self.assertAlmostEqual(x["2016-11-25"], 1.1024835489183247, places=5)
        self.assertAlmostEqual(x.pct_change().std(), 0.0098947443332281192, places=5)
        self.assertAlmostEqual(x.pct_change().mean(), 0.00011788200221789502, places=5)

    def test_assets(self):
        x = rand_assets(corr=np.array([[1.0,0.2],[0.2,1.0]]), drift=np.array([0.0,0.0]), sigma=np.array([0.01,0.02]), t0="2010-01-01", t1="2017-01-01")
        self.assertAlmostEqual(x.pct_change().corr()[0][1], 0.19524023082368255, places=5)
        self.assertAlmostEqual(x[1].pct_change().std(), 0.019553138398418057, places=5)
        self.assertAlmostEqual(x[0]["2016-11-25"], 0.7137679656717445, places=5)
