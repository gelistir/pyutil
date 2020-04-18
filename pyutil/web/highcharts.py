import pandas as pd


def to_json(series):
    return [[int(pd.Timestamp(t).value * 1e-6), float(value)] for t, value in series.dropna().items()]


def parse(value):
    return pd.Series({pd.Timestamp(1e6 * int(v[0])): float(v[1]) for v in value})
