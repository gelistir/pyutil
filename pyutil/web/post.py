from collections import OrderedDict

import pandas as pd
from pyutil.performance.summary import performance as perf
from pyutil.performance.month import monthlytable as mon
from pyutil.performance.drawdown import drawdown as dd


def _series2arrays(x, tz="CET"):
    # this function converts a pandas series into a dictionary of two arrays
    # to mock the behaviour of highcharts...
    def __f(x):
        return pd.Timestamp(x, tz=tz).value * 1e-6

    return {"time": [__f(key) for key in x.index], "data": x.values}


def _frame2dict(frame):
    return {'columns': list(frame.keys()),
            'data': [frame.loc[key].dropna().to_dict(into=OrderedDict) for key in frame.index]}


def _series2array(x, tz="CET"):
    """ convert a pandas series into an array suitable for HighCharts """

    def f(x):
        return pd.Timestamp(x, tz=tz).value * 1e-6

    return [[f(key), value] for key, value in x.dropna().items()]


def _arrays2series(data, tz="CET"):
    f = pd.Series(index=data["time"], data=data["data"])
    before = data.get("min", None)
    after = data.get("max", None)
    f = f.truncate(before=before, after=after)
    f.index = [pd.Timestamp(1e6 * a, tz=tz) for a in f.index]
    return f


def _name_value(x):
    return [{"name": key, "value": value} for key, value in x.items()]


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
    return {"data": _name_value(__performance(f))}


def post_month(data):
    # construct the pandas series again from "time" and "data" arrays
    f = _arrays2series(data)

    frame = 100 * mon(f)
    frame = frame.applymap("{0:.2f}%".format).replace("nan%", "")
    frame.index = ['{:d}'.format(year) for year in frame.index]
    frame = frame.reset_index().rename(columns={"index": "Year"})

    return _frame2dict(frame)


def post_drawdown(data):
    f = _arrays2series(data)
    # f is now a pandas series
    return {"data": _series2array(x=dd(price=f))}


def post_volatility(data):
    f = _arrays2series(data)
    r = f.pct_change().dropna()
    return {"data": _series2array(16*r.ewm(32).std())}


def post_nav(nav, name=""):
    data = _series2arrays(nav)
    return {"nav": _series2array(nav), "drawdown": post_drawdown(data)["data"], "volatility": post_volatility(data)["data"], "name": name}


