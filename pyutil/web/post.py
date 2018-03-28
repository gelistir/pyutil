import pandas as pd

from pyutil.performance.month import monthlytable as mon
from pyutil.performance.summary import performance as perf, fromNav


def _arrays2series(data, tz="CET"):
    f = pd.Series(index=data["time"], data=data["data"])
    before = data.get("min", None)
    after = data.get("max", None)
    f = f.truncate(before=before, after=after)
    f.index = [pd.Timestamp(1e6 * a, tz=tz) for a in f.index]
    return f


def post_perf(data):

    def __performance(nav):
        x = perf(nav)

        for key in x.index:
            if key in {"First_at", "Last_at"}:
                x[key] = x[key].strftime("%Y-%m-%d")
            elif key in {"# Events", "# Events per year", "# Positive Events", "# Negative Events"}:
                x[key] = '{:d}'.format(int(x[key]))
            else:
                x[key] = '{0:.2f}'.format(float(x[key]))

        return x

    f = _arrays2series(data)
    return __performance(f)


def post_month(data):
    # construct the pandas series again from "time" and "data" arrays
    f = _arrays2series(data)
    frame = mon(f)
    frame.index = ['{:d}'.format(year) for year in frame.index]
    return frame


def post_drawdown(data):
    f = fromNav(ts=_arrays2series(data))
    return f.drawdown


def post_volatility(data):
    f = fromNav(ts=_arrays2series(data))
    return f.ewm_volatility()

