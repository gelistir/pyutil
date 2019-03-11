import pandas as pd

def adjust(ts):
    try:
        return 100*ts/(ts.dropna().iloc[0])
    except IndexError:
        return pd.Series({})
