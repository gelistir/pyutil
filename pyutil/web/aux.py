import pandas as pd


def int2time(x, fmt="%Y-%m-%d"):
    try:
        return pd.Timestamp(int(x)*1e6).strftime(fmt)
    except ValueError:
        return ""


def double2percent(x):
    return "{0:.2f}%".format(float(100.0*x)).replace("nan%", "")

