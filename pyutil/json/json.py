import pandas as pd


def flatten(name, ts):
    assert isinstance(ts, pd.Series), "Flatten only works for a Series. Use frame.stack() eventually"
    return {"{0}.{1}".format(name, key): value for key, value in series2dict(ts).items()}


def series2dict(ts):
    if ts.index.nlevels == 1:
        return {"{0}".format(t.strftime("%Y%m%d")): v for t, v in ts.dropna().items()}
    else:
        return {"{0}.{1}".format(t[1], t[0].strftime("%Y%m%d")): v for t, v in ts.dropna().items()}


def frame2dict(frame):
    return {key: series2dict(series) for key, series in frame.iteritems()}
