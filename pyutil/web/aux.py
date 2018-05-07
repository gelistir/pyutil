import json
from collections import OrderedDict

import pandas as pd


# def frame2dict(frame):
#     """ Pandas Dataframe to dict with "columns" and "data" """
#     return {'columns': list(frame.keys()),
#             'data': [frame.loc[key].dropna().to_dict(into=OrderedDict) for key in frame.index]}


def rest2data(request):
    return json.loads(request.data.decode("utf-8"))


def int2time(x, fmt="%Y-%m-%d"):
    try:
        return pd.Timestamp(int(x)*1e6).strftime(fmt)
    except ValueError:
        return ""


def double2percent(x):
    return "{0:.2f}%".format(float(100.0*x)).replace("nan%", "")

