import pandas as pd
from unittest import TestCase

from pyutil.latex.util import trend, percentage
import pandas.util.testing as pdt


class TestLatex(TestCase):
    def test_trend(self):
        x = pd.Series(data=[0.0, 0.4, 0.5, 0.8, 1.0])
        self.assertEqual(trend(x),r"""\begin{sparkline}{20} \sparkrectangle 0.0 1.0 \spark 0.0 0.0     0.25 0.4     0.5 0.5     0.75 0.8     1.0 1.0    / \end{sparkline}""")
        pdt.assert_series_equal(x.apply(percentage), pd.Series(data=["0.00\%", "40.00\%", "50.00\%", "80.00\%", "100.00\%"]))

        x = pd.DataFrame(data=[0.0, 0.4, 0.5, 0.8, 1.0])
        pdt.assert_frame_equal(x.applymap(percentage), pd.DataFrame(data=["0.00\%", "40.00\%", "50.00\%", "80.00\%", "100.00\%"]))

