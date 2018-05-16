from unittest import TestCase

from pyutil.web.engine import month, performance, drawdown, volatility
from test.config import read_frame
from pandasweb.highcharts import series2arrays
from pandasweb.rest import data2request


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        ts = read_frame("price.csv")["A"].dropna()
        cls.request = data2request(data=series2arrays(ts))

    def test_month(self):
        x = month(self.request)
        self.assertListEqual(x["columns"], ['Year', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'STDev', 'YTD'])
        self.assertDictEqual(x["data"][0], {'Apr': '1.43%', 'Aug': '', 'Dec': '', 'Feb': '-5.50%', 'Jan': '8.35%', 'Jul': '', 'Jun': '', 'Mar': '-2.43%', 'May': '', 'Year': '2015', 'Nov': '', 'Oct': '', 'STDev': '20.69%', 'Sep': '', 'YTD': '1.33%'})

    def test_performance(self):
        x = performance(self.request)
        self.assertListEqual(x["data"], [{'name': 'Return', 'value': '-28.27'}, {'name': '# Events', 'value': '601'},
                                         {'name': '# Events per year', 'value': '261'},
                                         {'name': 'Annua Return', 'value': '-14.43'},
                                         {'name': 'Annua Volatility', 'value': '18.03'},
                                         {'name': 'Annua Sharpe Ratio (r_f = 0)', 'value': '-0.80'},
                                         {'name': 'Max Drawdown', 'value': '32.61'},
                                         {'name': 'Max % return', 'value': '4.07'},
                                         {'name': 'Min % return', 'value': '-9.11'}, {'name': 'MTD', 'value': '1.43'},
                                         {'name': 'YTD', 'value': '1.33'}, {'name': 'Current Nav', 'value': '0.72'},
                                         {'name': 'Max Nav', 'value': '1.01'},
                                         {'name': 'Current Drawdown', 'value': '29.07'},
                                         {'name': 'Calmar Ratio (3Y)', 'value': '-0.44'},
                                         {'name': '# Positive Events', 'value': '285'},
                                         {'name': '# Negative Events', 'value': '316'},
                                         {'name': 'Value at Risk (alpha = 95)', 'value': '1.91'},
                                         {'name': 'Conditional Value at Risk (alpha = 95)', 'value': '2.76'},
                                         {'name': 'First_at', 'value': '2013-01-01'},
                                         {'name': 'Last_at', 'value': '2015-04-22'}])

    def test_drawdown(self):
        x = drawdown(self.request)
        self.assertEqual(x[2][0],  1357171200000)
        self.assertAlmostEqual(x[2][1], 0.0136048372754759, places=10)

    def test_volatility(self):
        x = volatility(self.request)
        self.assertEqual(x[2][0],  1363219200000)
        self.assertAlmostEqual(x[2][1], 0.11651354870460683, places=10)





