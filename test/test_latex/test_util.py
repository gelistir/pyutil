import pandas as pd
from unittest import TestCase

from pyutil.latex.util import trend


class TestLatex(TestCase):
    def test_trend(self):
        x = pd.Series(data=[0.0, 0.4, 0.5, 0.8, 1.0])
        self.assertEqual(trend(x),r"""\begin{sparkline}{20} \sparkrectangle 0.0 1.0 \spark 0.0 0.0     0.25 0.4     0.5 0.5     0.75 0.8     1.0 1.0    / \end{sparkline}""")
