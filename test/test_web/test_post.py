from unittest import TestCase

import pandas as pd

from pyutil.web.aux import reset_index
from pyutil.web.post import post_month, post_perf, post_drawdown, post_volatility, post_nav, _series2arrays
from test.config import read_series


class TestPost(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nav = read_series("nav.csv", parse_dates=True)
        cls.data = _series2arrays(cls.nav)

    def test_post_month(self):
        x = post_month(data=self.data)
        self.assertDictEqual(x, {'columns': ['Year', 'Jan', 'Feb', 'Dec', 'STDev', 'YTD'],
                                 'data': [{'Year': '2015', 'Jan': '0.93%', 'Feb': '0.59%', 'Dec': '', 'STDev': '0.83%',
                                           'YTD': '1.53%'},
                                          {'Year': '2014', 'Jan': '', 'Feb': '', 'Dec': '-0.04%', 'STDev': '',
                                           'YTD': '-0.04%'}]})

    def test_post_perf(self):
        x = post_perf(data=self.data)
        self.assertListEqual(x["data"], [{'name': 'Return', 'value': '1.49'}, {'name': '# Events', 'value': '40'},
                                         {'name': '# Events per year', 'value': '261'},
                                         {'name': 'Annua Return', 'value': '9.64'},
                                         {'name': 'Annua Volatility', 'value': '3.02'},
                                         {'name': 'Annua Sharpe Ratio (r_f = 0)', 'value': '3.19'},
                                         {'name': 'Max Drawdown', 'value': '0.89'},
                                         {'name': 'Max % return', 'value': '0.39'},
                                         {'name': 'Min % return', 'value': '-0.53'}, {'name': 'MTD', 'value': '0.59'},
                                         {'name': 'YTD', 'value': '1.53'}, {'name': 'Current Nav', 'value': '1.31'},
                                         {'name': 'Max Nav', 'value': '1.31'},
                                         {'name': 'Current Drawdown', 'value': '0.00'},
                                         {'name': 'Calmar Ratio (3Y)', 'value': '10.84'},
                                         {'name': '# Positive Events', 'value': '22'},
                                         {'name': '# Negative Events', 'value': '18'},
                                         {'name': 'Value at Risk (alpha = 95)', 'value': '0.33'},
                                         {'name': 'Conditional Value at Risk (alpha = 95)', 'value': '0.43'},
                                         {'name': 'First_at', 'value': '2014-12-11'},
                                         {'name': 'Last_at', 'value': '2015-02-05'}])

    def test_post_drawdown(self):
        x = post_drawdown(data=self.data)
        self.assertEqual(len(x["data"]), 41)
        self.assertAlmostEqual(x["data"][20][1], 0.00039687542576194446, places=10)

    def test_post_volatility(self):
        x = post_volatility(data=self.data)
        self.assertEqual(len(x["data"]), 39)
        self.assertAlmostEqual(x["data"][20][1], 0.0300260038627532, places=10)

    def test_post_nav(self):
        x = post_nav(nav=self.nav, name="Peter Maffay")
        self.assertEqual(x["name"], "Peter Maffay")
        self.assertSetEqual(set(x.keys()), {"name", "nav", "drawdown", "volatility"})

        self.assertAlmostEqual(x["drawdown"][20][1], 0.00039687542576194446, places=10)
        self.assertAlmostEqual(x["volatility"][20][1], 0.0300260038627532, places=10)

    def test_reset_index(self):
        x = pd.DataFrame(index=["A"], columns=["Wurst"], data=[[2.0]])
        b = reset_index(x)
        self.assertListEqual(b["columns"], ["Name", "Wurst"])

