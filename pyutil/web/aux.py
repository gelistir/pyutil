import json
from collections import OrderedDict

import pandas as pd
from pyutil.performance.summary import performance as perf
from pyutil.performance.month import monthlytable as mon


def frame2dict(frame):
    return {'columns': list(frame.keys()), 'data': [frame.loc[key].dropna().to_dict(into=OrderedDict) for key in frame.index]}


def series2array(x, tz="CET"):
    """ convert a pandas series into an array suitable for HighCharts """
    def f(x):
        return pd.Timestamp(x, tz=tz).value*1e-6

    return [[f(key), value] for key, value in x.dropna().items()]


def rest2data(request):
    return json.loads(request.data.decode("utf-8"))


def __arrays2series(data, tz="CET"):
    f = pd.Series(index=data["time"], data=data["data"])
    before = data.get("min", None)
    after = data.get("max", None)
    f = f.truncate(before=before, after=after)
    f.index = [pd.Timestamp(1e6 * a, tz=tz) for a in f.index]
    return f


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


def __name_value(x):
    return [{"name": key, "value": value} for key, value in x.items()]


def post_perf(data):
    f = __arrays2series(data)
    return __name_value(__performance(f))


def post_month(data):
    # construct the pandas series again from "time" and "data" arrays
    f = __arrays2series(data)

    frame = 100 * mon(f)
    frame = frame.applymap("{0:.2f}%".format).replace("nan%", "")
    frame.index = ['{:d}'.format(year) for year in frame.index]
    frame = frame.reset_index().rename(columns={"index": "Year"})

    return frame2dict(frame)
