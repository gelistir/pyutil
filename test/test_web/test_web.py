from unittest import TestCase

import pandas as pd

from test.config import read_series

nav = read_series("nav.csv", parse_dates=True)

from pyutil.web.aux import series2array, monthlytable, performance, name_value


class TestWeb(TestCase):
    def test_series2array(self):
        x = series2array(nav)
        self.assertEqual(len(x), 41)
        self.assertEqual(x[0], [1418252400000, 1.2940225007396362])

    def test_monthlytable(self):
        x = monthlytable(nav)
        self.assertSetEqual(set(x.keys()), {"columns", "data"})
        self.assertListEqual(x["columns"], ['Year', 'Jan', 'Feb', 'Dec', 'STDev', 'YTD'])



    def test_performance(self):
        x = performance(nav)
        self.assertDictEqual(x.to_dict(), {'# Events': '40', '# Events per year': '261', '# Negative Events': '18',
                                           '# Positive Events': '22', 'Annua Return': '9.64',
                                           'Annua Sharpe Ratio (r_f = 0)': '3.19', 'Annua Volatility': '3.02',
                                           'Calmar Ratio (3Y)': '10.84',
                                           'Conditional Value at Risk (alpha = 95)': '0.43',
                                           'Current Drawdown': '0.00', 'Current Nav': '1.31',
                                           'First_at': '2014-12-11', 'Last_at': '2015-02-05', 'MTD': '0.59',
                                           'Max % return': '0.39',
                                           'Max Drawdown': '0.89', 'Max Nav': '1.31', 'Min % return': '-0.53',
                                           'Return': '1.49', 'Value at Risk (alpha = 95)': '0.33', 'YTD': '1.53'})

    def test_name_value(self):
        x = performance(nav)
        self.assertListEqual(name_value(x), [{'name': 'Return', 'value': '1.49'}, {'name': '# Events', 'value': '40'},
                                             {'name': '# Events per year', 'value': '261'},
                                             {'name': 'Annua Return', 'value': '9.64'},
                                             {'name': 'Annua Volatility', 'value': '3.02'},
                                             {'name': 'Annua Sharpe Ratio (r_f = 0)', 'value': '3.19'},
                                             {'name': 'Max Drawdown', 'value': '0.89'},
                                             {'name': 'Max % return', 'value': '0.39'},
                                             {'name': 'Min % return', 'value': '-0.53'},
                                             {'name': 'MTD', 'value': '0.59'}, {'name': 'YTD', 'value': '1.53'},
                                             {'name': 'Current Nav', 'value': '1.31'},
                                             {'name': 'Max Nav', 'value': '1.31'},
                                             {'name': 'Current Drawdown', 'value': '0.00'},
                                             {'name': 'Calmar Ratio (3Y)', 'value': '10.84'},
                                             {'name': '# Positive Events', 'value': '22'},
                                             {'name': '# Negative Events', 'value': '18'},
                                             {'name': 'Value at Risk (alpha = 95)', 'value': '0.33'},
                                             {'name': 'Conditional Value at Risk (alpha = 95)', 'value': '0.43'},
                                             {'name': 'First_at', 'value': '2014-12-11'},
                                             {'name': 'Last_at', 'value': '2015-02-05'}])

    def test_date(self):
        x = pd.Series(index=[pd.Timestamp("2020-01-01").date(), pd.Timestamp("2020-01-02").date()], data=[5.0, 6.0])
        y = pd.Series(index=[pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")], data=[5.0, 6.0])

        self.assertEqual(series2array(x)[0][0], 1577833200000)
        self.assertListEqual(series2array(x), series2array(y))
        self.assertEqual(pd.Timestamp(1577833200000*1e6, tz="CET").date(), pd.Timestamp("2020-01-01").date())
        print(x.to_json())
        print(series2array(x))


    def test_frame(self):
        x = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11").date()], data=[[2.0]])
        y = pd.DataFrame(index=["Asset A"], columns=[pd.Timestamp("2015-04-11")], data=[[2.0]])
        self.assertEqual(x.to_json(), y.to_json())
