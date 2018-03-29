import pandas as pd

from pyutil.performance.month import monthlytable as mon
from pyutil.performance.summary import performance as perf, fromNav
from pyutil.web.aux import rest2data, reset_index, double2percent, series2array


def _arrays2series(data, tz="CET", date=False):
    # data has to be an array
    assert isinstance(data, dict)

    # to compile the series we need...
    assert "time" in data.keys()
    assert "data" in data.keys()

    # eh voila...
    f = pd.Series(index=data["time"], data=data["data"])

    # there might be min and max specified (e.g. if applied to a Highcharts subset)
    before = data.get("min", None)
    after = data.get("max", None)

    f = f.truncate(before=before, after=after)

    # get into proper Timespace
    if date:
        f.index = [pd.Timestamp(1e6 * a, tz=tz).date() for a in f.index]
    else:
        f.index = [pd.Timestamp(1e6 * a, tz=tz) for a in f.index]

    return f


def month(request, date=False):
    # extract data of incoming request
    data = rest2data(request)

    series = _arrays2series(data, date=date)

    frame = mon(series)
    frame.index = ['{:d}'.format(year) for year in frame.index]

    # construct the frame, return the "data, columns" dictionary...
    return reset_index(frame=frame.applymap(double2percent), index="Year")


def performance(request, date=False):
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

    # extract the underlying data of the request.
    data = rest2data(request)

    # the pandas Series
    series = _arrays2series(data, date=date)

    # the results as a dictionary
    return {"data": [{"name": name, "value": value} for name, value in __performance(nav=series).items()]}


def drawdown(request, date=False):
    data = rest2data(request)
    # the pandas Series
    series = fromNav(_arrays2series(data, date=date))
    return series2array(series.drawdown)


def volatility(request, com=50, min_periods=50, date=False):
    data = rest2data(request)
    # the pandas Series
    series = fromNav(_arrays2series(data, date=date))
    return series2array(series.ewm_volatility(com=com, min_periods=min_periods))
