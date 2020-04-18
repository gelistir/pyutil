from pyutil.mongo.engine.frame import Frame
from test.config import *

import pandas.util.testing as pdt


def test_frame():
    f = Frame(name="Portfolio", prices=read_pd("price.csv", index_col=0, parse_dates=True))
    pdt.assert_frame_equal(f.prices, read_pd("price.csv", index_col=0, parse_dates=True))
