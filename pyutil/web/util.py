import pandas as pd
from pyutil.performance.summary import fromNav


def __percentage(x):
    return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")


def month(series):
    if series.empty:
        return pd.DataFrame({})
    else:
        frame = fromNav(series).monthlytable.applymap(__percentage)
        frame.index = ['{:d}'.format(year) for year in frame.index]
        return frame


def performance(series):
    def __f(x):
        for key in x.index:
            if key in {"First at", "Last at"}:
                x[key] = x[key].strftime("%Y-%m-%d")
            elif key in {"# Events", "# Events per year", "# Positive Events", "# Negative Events"}:
                x[key] = '{:d}'.format(int(x[key]))
            else:
                x[key] = '{0:.2f}'.format(float(x[key]))

        return x

    if series.empty:
        return []
    else:
        p = __f(fromNav(series).summary())
        # the results as a dictionary
        return [{'name': name, 'value': value} for name, value in p.items()]