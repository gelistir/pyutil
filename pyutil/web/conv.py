import pandas as pd


def series2array(x, tz="CET"):
    return [[pd.Timestamp(key, tz=tz).value * 1e-6, value] for key, value in x.dropna().items()]