import pandas as pd
import pandas.util.testing as pdt

import pytest
from pyutil.web.highcharts import to_json, parse


@pytest.fixture
def nav():
    return pd.Series({pd.Timestamp('2014-12-11'): 1.29, pd.Timestamp('2014-12-12'): 1.29, pd.Timestamp('2014-12-15'): 1.28})


def test_parser(nav):
    pdt.assert_series_equal(parse(to_json(nav)), nav)