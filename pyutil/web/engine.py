from pyutil.performance.month import monthlytable as mon
from pyutil.performance.summary import performance as perf, fromNav
from pyutil.web.aux import rest2data, double2percent

from pandasweb.highcharts import series2array, arrays2series
from pandasweb.frames import frame2dict


def month(request, date=False):
    # extract data of incoming request
    data = rest2data(request)

    series = arrays2series(data, date=date)

    frame = mon(series)
    frame.index = ['{:d}'.format(year) for year in frame.index]

    # construct the frame, return the "data, columns" dictionary...
    frame = frame.applymap(double2percent)
    frame.index.names = ["Year"]
    frame = frame.reset_index()
    return frame2dict(frame) #reset_index(frame=frame.applymap(double2percent), index="Year")


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
    series = arrays2series(data, date=date)

    # the results as a dictionary
    return {"data": [{"name": name, "value": value} for name, value in __performance(nav=series).items()]}


def drawdown(request, date=False):
    data = rest2data(request)
    # the pandas Series
    series = fromNav(arrays2series(data, date=date))
    return series2array(series.drawdown)


def volatility(request, com=50, min_periods=50, date=False):
    data = rest2data(request)
    # the pandas Series
    series = fromNav(arrays2series(data, date=date))
    return series2array(series.ewm_volatility(com=com, min_periods=min_periods))
