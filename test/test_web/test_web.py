from unittest import TestCase
import pandas as pd
from test.config import read_series

nav = read_series("nav.csv", parse_dates=True)

from pyutil.web.aux import series2array, post_month, post_perf, frame2dict


def _series2arrays(x, tz="CET"):
    # this function converts a pandas series into a dictionary of two arrays
    # to mock the behaviour of highcharts...
    def f(x):
        return pd.Timestamp(x, tz=tz).value*1e-6

    return {"time": [f(key) for key in x.index], "data": x.values}


class TestWeb(TestCase):
    def test_series2array(self):
        x = series2array(nav)
        self.assertEqual(len(x), 41)
        self.assertEqual(x[0], [1418252400000, 1.2940225007396362])

    def test_frame2dict(self):
        x = nav.to_frame(name="nav")
        self.assertIsInstance(x, pd.DataFrame)
        y = frame2dict(x)

        self.assertSetEqual({"columns", "data"}, set(y.keys()))
        self.assertListEqual(y["columns"], ["nav"])
        for n, a in enumerate(y["data"]):
            self.assertAlmostEqual(a["nav"], x.values[n], places=10)

    def test_post_month(self):
        x = post_month(data=_series2arrays(nav))
        self.assertDictEqual(x, {'columns': ['Year', 'Jan', 'Feb', 'Dec', 'STDev', 'YTD'],
                                 'data': [{'Year': '2015', 'Jan': '0.95%', 'Feb': '0.59%', 'Dec': '', 'STDev': '0.86%', 'YTD': '1.54%'},
                                          {'Year': '2014', 'Jan': '', 'Feb': '', 'Dec': '-0.06%', 'STDev': '', 'YTD': '-0.06%'}]})

    def test_post_perf(self):
        x = post_perf(data=_series2arrays(nav))
        self.assertListEqual(x, [{'name': 'Return', 'value': '1.49'}, {'name': '# Events', 'value': '40'},
                                 {'name': '# Events per year', 'value': '261'},
                                 {'name': 'Annua Return', 'value': '9.64'},
                                 {'name': 'Annua Volatility', 'value': '3.02'},
                                 {'name': 'Annua Sharpe Ratio (r_f = 0)', 'value': '3.19'},
                                 {'name': 'Max Drawdown', 'value': '0.89'}, {'name': 'Max % return', 'value': '0.39'},
                                 {'name': 'Min % return', 'value': '-0.53'}, {'name': 'MTD', 'value': '0.59'},
                                 {'name': 'YTD', 'value': '1.54'}, {'name': 'Current Nav', 'value': '1.31'},
                                 {'name': 'Max Nav', 'value': '1.31'}, {'name': 'Current Drawdown', 'value': '0.00'},
                                 {'name': 'Calmar Ratio (3Y)', 'value': '10.84'},
                                 {'name': '# Positive Events', 'value': '22'},
                                 {'name': '# Negative Events', 'value': '18'},
                                 {'name': 'Value at Risk (alpha = 95)', 'value': '0.33'},
                                 {'name': 'Conditional Value at Risk (alpha = 95)', 'value': '0.43'},
                                 {'name': 'First_at', 'value': '2014-12-10'},
                                 {'name': 'Last_at', 'value': '2015-02-04'}])

    def test_date(self):
        x = pd.Series(index=[pd.Timestamp("2020-01-01").date(), pd.Timestamp("2020-01-02").date()], data=[5.0, 6.0])
        y = pd.Series(index=[pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")], data=[5.0, 6.0])

        self.assertEqual(series2array(x)[0][0], 1577833200000)
        self.assertListEqual(series2array(x), series2array(y))
        self.assertEqual(pd.Timestamp(1577833200000*1e6, tz="CET").date(), pd.Timestamp("2020-01-01").date())

    def test_frame(self):
        x = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11").date()], data=[[2.0]])
        y = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11")], data=[[2.0]])
        self.assertEqual(x.to_json(), y.to_json())

