import json
from collections import OrderedDict

import pandas as pd


def frame2dict(frame):
    """ Pandas Dataframe to dict with "columns" and "data" """
    return {'columns': list(frame.keys()),
            'data': frame.to_dict('records')}


def series2array(x, tz="CET"):
    """ convert a pandas series into an array suitable for HighCharts """

    def __f(x):
        return pd.Timestamp(x, tz=tz).value * 1e-6

    return [[__f(key), value] for key, value in x.dropna().items()]


def rest2data(request):
    return json.loads(request.data.decode("utf-8"))


def reset_index(frame, index="Name"):
    """ Prepare a pandas Dataframe for """
    f = frame.reset_index().rename(columns={"index": index})
    return frame2dict(f)


def int2time(x, fmt="%Y-%m-%d"):
    try:
        return pd.Timestamp(int(x)*1e6).strftime(fmt)
    except ValueError:
        return ""


def double2percent(x):
    return "{0:.2f}%".format(float(100.0*x)).replace("nan%", "")

