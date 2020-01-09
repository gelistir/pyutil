import numpy as np
from pyutil.portfolio.format import fint, fdouble, percentage


def test_percentage():
    assert percentage(0.21) == "21.00%"
    assert percentage(np.nan) == ""


def test_fdouble():
    assert fdouble(0.22312) == "0.22"
    assert fdouble(np.nan) == ""


def test_fint():
    assert fint(152) == "152"
    assert fint(np.nan) == ""

