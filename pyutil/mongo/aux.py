import pandas as pd


def flatten(name, ts):
    if isinstance(ts, pd.Series):
        a = ts.copy().dropna()
        a.index = ["{0}.{1}".format(name, t.strftime("%Y%m%d")) for t in a.index]
        return {"$set": a.to_dict()}

    if isinstance(ts, pd.DataFrame):
        a = ts.copy().stack().dropna()
        a.index = ["{0}.{1}.{2}".format(name, t[1], t[0].strftime("%Y%m%d")) for t in a.index]
        return {"$set": a.to_dict()}

    raise TypeError("ts is of type {0}".format(type(ts)))


def series2dict(ts):
    return {"{0}".format(t.strftime("%Y%m%d")): v for t, v in ts.dropna().iteritems()}