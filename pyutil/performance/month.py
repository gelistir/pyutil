import calendar
import numpy as np


def monthlytable(nav):
    """
    Get a table of monthly returns

    :param nav:

    :return:
    """
    r = nav.pct_change().dropna()
    # Works better in the first month
    # Compute all the intramonth-returns, instead of reapplying some monthly resampling of the NAV
    return_monthly = r.groupby([lambda x: x.year, lambda x: x.month]).apply(lambda x: (1 + x).prod() - 1.0)
    frame = return_monthly.unstack(level=1).rename(columns=lambda x: calendar.month_abbr[x])
    a = (frame + 1.0).prod(axis=1) - 1.0
    frame["STDev"] = np.sqrt(12) * frame.std(axis=1)
    # make sure that you don't include the column for the STDev in your computation
    frame["YTD"] = a
    frame.index.name = "Year"
    # most recent years on top
    return frame.iloc[::-1]