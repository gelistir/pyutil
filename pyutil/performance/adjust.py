import pandas as pd


def adjust(ts: pd.Series, value=100.0) -> pd.Series:
    try:
        return value * ts / (ts.dropna().iloc[0])
    except IndexError:
        return pd.Series({})
