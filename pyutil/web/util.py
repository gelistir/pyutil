import pandas as pd
from pyutil.performance.summary import fromNav


def performance(series):
    # def __f(x):
    #     for key in x.index:
    #         if key in {"First at", "Last at"}:
    #             x[key] = x[key].strftime("%Y-%m-%d")
    #         elif key in {"# Events", "# Events per year", "# Positive Events", "# Negative Events"}:
    #             x[key] = '{:d}'.format(int(x[key]))
    #         else:
    #             x[key] = '{0:.2f}'.format(float(x[key]))
    #
    #     return x

    if series.empty:
        return pd.Series({})
    else:
        return fromNav(series).summary()