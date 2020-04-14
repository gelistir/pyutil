import tempfile

import pandas as pd
import pytest

from pyutil.plot.plots import container, nav_curve, display_monthtable, display_performance, pandas_display
from test.config import read


@pytest.fixture()
def ts():
    ts = read("ts.csv", parse_dates=True, squeeze=True, header=None, index_col=0)
    ts.name = None
    ts.index.name = None
    return ts


def test_container():
    a = container()
    assert a


def test_nav_curve(ts):
    x = nav_curve(ts.tail(5))
    assert x.model["log_x"] == False
    assert x.model["log_y"] == False
    assert len(x.model["rangeAxes"]) == 2


def test_month_table(ts):
    display_monthtable(ts)


def test_display_performance(ts):
    x = display_performance(ts.to_frame(name="NAV"))
    assert x.model


def test_frames():
    f = pd.DataFrame(index=["A","B"], columns=["C1", "C2"], data=[["A","B"],["C","D"]])
    assert pandas_display(f, file=tempfile.NamedTemporaryFile(suffix="csv").name)