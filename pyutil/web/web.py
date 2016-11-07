import numpy as np
import pandas as pd
from ..performance.performance import performance

TABLE_FORMAT = "table table-striped table-bordered"


def series2array(x, tz="CET"):
    return [[pd.Timestamp(key, tz=tz).value * 1e-6, value] for key, value in x.dropna().items()]


def performance2json(x, decimals=2, dateformat="%Y-%m-%d", days=262):
    p = performance(x, days=days)
    for i in p.index:
        if isinstance(p[i], np.float64):
            p[i] = np.round(p[i], decimals=decimals)

    p["Last"] = pd.Timestamp(p["Last"]).date().strftime(dateformat)
    p["First"] = pd.Timestamp(p["First"]).date().strftime(dateformat)
    p.name = "performance"
    return p.apply(str).to_json(orient="split")


def frame2html(frame, name, format=TABLE_FORMAT):
    pd.set_option('display.max_colwidth', -1)

    return frame.to_html(classes="{0} {1}".format(name, format),
                         float_format=lambda x: '{0:.2f}'.format(x) if pd.notnull(x) else '-', escape=False)
