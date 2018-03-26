import pandas as pd
from pyutil.performance.summary import performance as perf
from pyutil.performance.month import monthlytable as mon


def series2array(x, tz="CET"):
    """ convert a pandas series into an array suitable for HighCharts """
    def f(x):
        return int(pd.Timestamp(x, tz=tz).value*1e-6)

    return [[f(key), value] for key, value in x.items()]


def monthlytable(nav):
    frame = 100 * mon(nav)
    frame = frame.applymap("{0:.2f}%".format).replace("nan%", "")
    frame.index = ['{:d}'.format(year) for year in frame.index]
    frame = frame.reset_index().rename(columns={"index": "Year"})
    return {"columns": list(frame.keys()), "data": [row.dropna().to_dict() for i, row in frame.iterrows()]}


def performance(nav):
    x = perf(nav)

    for key in x.index:
        if key in {"First_at", "Last_at"}:
            x[key] = x[key].strftime("%Y-%m-%d")
        elif key in {"# Events", "# Events per year", "# Positive Events", "# Negative Events"}:
            x[key] = '{:d}'.format(int(x[key]))
        else:
            x[key] = '{0:.2f}'.format(float(x[key]))

    return x


def name_value(x):
    return [{"name": key, "value": value} for key, value in x.items()]
