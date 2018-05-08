from pandasweb.frames import frame2dict
from pandasweb.highcharts import series2array, arrays2series
from pandasweb.rest import request2data

from pyutil.performance.summary import fromNav

def __series(request, date=False):
    data = request2data(request)
    # the pandas Series
    return fromNav(arrays2series(data, date=date))

def month(request, date=False):
    def double2percent(x):
        return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

    # extract data of incoming request
    frame = __series(request, date=date).monthlytable
    frame.index = ['{:d}'.format(year) for year in frame.index]

    # construct the frame, return the "data, columns" dictionary...
    frame = frame.applymap(double2percent)
    frame.index.names = ["Year"]
    frame = frame.reset_index()
    return frame2dict(frame)


def performance(request, date=False):
    def __f(x):
        for key in x.index:
            if key in {"First_at", "Last_at"}:
                x[key] = x[key].strftime("%Y-%m-%d")
            elif key in {"# Events", "# Events per year", "# Positive Events", "# Negative Events"}:
                x[key] = '{:d}'.format(int(x[key]))
            else:
                x[key] = '{0:.2f}'.format(float(x[key]))

        return x

    p = __f(__series(request, date=date).summary())

    # the results as a dictionary
    return {"data": [{"name": name, "value": value} for name, value in p.items()]}


def drawdown(request, date=False):
    return series2array(__series(request, date=date).drawdown)


def volatility(request, com=50, min_periods=50, date=False):
    return series2array(__series(request, date=date).ewm_volatility(com=com, min_periods=min_periods))
