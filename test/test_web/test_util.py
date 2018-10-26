import unittest

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

#from pyserver.util import month, performance

#from test.settings import read_frame, test_portfolio


# first import settings as we define environment variables in there...
from pyutil.web.util import month, performance
from test.config import test_portfolio, read_frame


class TestUtil(unittest.TestCase):
    def test_month(self):
        p = test_portfolio()
        print(month(p.nav))
        print(read_frame(name="month.csv"))

        x = read_frame(name="month.csv", parse_dates=False).replace(np.NaN, "")
        x.index = ['{:d}'.format(year) for year in x.index]

        pdt.assert_frame_equal(month(p.nav), x)

    def test_month_empty(self):
        self.assertTrue(month(pd.Series({})).empty)

    def test_performance(self):
        prices = read_frame("price.csv")["A"]
        x = performance(series=prices)

        f = [{'name': 'Return', 'value': '-28.27'}, {'name': '# Events', 'value': '601'},
         {'name': '# Events per year', 'value': '261'}, {'name': 'Annua Return', 'value': '-14.43'},
         {'name': 'Annua Volatility', 'value': '18.03'}, {'name': 'Annua Sharpe Ratio (r_f = 0)', 'value': '-0.80'},
         {'name': 'Max Drawdown', 'value': '32.61'}, {'name': 'Max % return', 'value': '4.07'},
         {'name': 'Min % return', 'value': '-9.11'}, {'name': 'MTD', 'value': '1.43'}, {'name': 'YTD', 'value': '1.33'},
         {'name': 'Current Nav', 'value': '1200.59'}, {'name': 'Max Nav', 'value': '1692.70'},
         {'name': 'Current Drawdown', 'value': '29.07'}, {'name': 'Calmar Ratio (3Y)', 'value': '-0.44'},
         {'name': '# Positive Events', 'value': '285'}, {'name': '# Negative Events', 'value': '316'},
         {'name': 'Value at Risk (alpha = 95)', 'value': '1.91'},
         {'name': 'Conditional Value at Risk (alpha = 95)', 'value': '2.76'},
         {'name': 'First at', 'value': '2013-01-01'}, {'name': 'Last at', 'value': '2015-04-22'}]

        self.assertListEqual(x, f)

    def test_performance_empty(self):
        prices = pd.Series({})
        x = performance(series=prices)
        self.assertListEqual(x, [])
