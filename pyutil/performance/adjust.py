import pandas as pd


def adjust(ts: pd.Series) -> pd.Series:
    try:
        return 100 * ts / (ts.dropna().iloc[0])
    except IndexError:
        return pd.Series({})
