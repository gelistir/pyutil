import requests_mock

import pandas as pd
from pyutil.web.requests import fetch_csv, fetch_json
import pandas.util.testing as pdt


frame = pd.DataFrame(index=["A", "B"], columns=["C1"], data=[[2], [3]])


def test_fetch_csv():
    with requests_mock.Mocker() as m:
        m.get("https://maffay.com", content=frame.to_csv().encode())
        pdt.assert_frame_equal(fetch_csv("https://maffay.com", index_col=0), frame)


def test_fetch_json():
    with requests_mock.Mocker() as m:
        m.get("https://maffay.com", json=frame.to_json(orient="table"))
        pdt.assert_frame_equal(fetch_json(url="https://maffay.com", orient="table"), frame)

