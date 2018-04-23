from unittest import TestCase
from test.config import read_series

nav = read_series("nav.csv", parse_dates=True)

from pyutil.timeseries.timeseries import adjust, ytd, mtd, consecutive
import pandas as pd
import pandas.util.testing as pdt


class TestTimeseries(TestCase):
    def test_adjust(self):
        x = adjust(nav.truncate(before="2015-01-01"))
        self.assertEqual(x.loc[x.index[0]], 1.0)

    def test_ytd(self):
        a = ytd(nav, today=pd.Timestamp("2014-05-07"))
        pdt.assert_series_equal(a, nav.truncate(before="2014-01-01", after="2014-05-07"))


    def test_mtd(self):
        a = mtd(nav, today=pd.Timestamp("2015-02-10"))
        pdt.assert_series_equal(a, nav.truncate(before="2015-02-01", after="2015-02-10"))

    def test_consecutive(self):
        x = pd.Series(index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], data=[1, 0, 0, 1, 1, 1, 0, 0, 1, 1])
        pdt.assert_series_equal(consecutive(x), pd.Series(index=x.index, data=[1, 0, 0, 1, 2, 3, 0, 0, 1, 2]))


    def test_mtd_date(self):
        # works also for dates
        a = pd.Series({pd.Timestamp("2010-01-05").date(): 2.0,
                       pd.Timestamp("2012-02-13").date(): 3.0,
                       pd.Timestamp("2012-02-14").date(): 4.0
                       })


        b = mtd(a, today=pd.Timestamp("2012-02-14").date())
        pdt.assert_series_equal(b, pd.Series({pd.Timestamp("2012-02-13").date(): 3.0,
                       pd.Timestamp("2012-02-14").date(): 4.0
                       }))

        c = ytd(a, today=pd.Timestamp("2012-04-14").date())
        pdt.assert_series_equal(c, pd.Series({pd.Timestamp("2012-02-13").date(): 3.0,
                       pd.Timestamp("2012-02-14").date(): 4.0
                       }))
