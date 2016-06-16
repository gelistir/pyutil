import numpy as np
import pandas as pd
from unittest import TestCase
from pyutil.pretty.pretty import PrettyPandas


class TestPretty(TestCase):
    def test_summary(self):
        A = pd.DataFrame(np.random.randn(5, 2))
        f = PrettyPandas(A).median().average().average(axis=1).std(axis=1).frame
        self.assertAlmostEqual(f[0]["Median"], A.median(axis=0)[0], places=10)
        self.assertAlmostEqual(f[0]["Average"], A.mean(axis=0)[0], places=10)
