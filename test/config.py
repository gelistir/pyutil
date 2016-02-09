import os
import pandas as pd


BASE_DIR = os.path.dirname(__file__)


def read_frame(name, parse_dates=True):
    return pd.read_csv(os.path.join(BASE_DIR, "resources", name), index_col=0, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True):
    return pd.read_csv(os.path.join(BASE_DIR, "resources", name), index_col=0, header=None, squeeze=True, parse_dates=parse_dates)




