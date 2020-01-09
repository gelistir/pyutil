import requests
import pandas as pd
from pyutil.web.requests import fetch_csv
import pandas.util.testing as pdt

# custom class to be the mock return value
# will override the requests.Response returned from requests.get
class MockResponse:

    @property
    def content(self):
        frame = pd.DataFrame(index=["A","B"], columns=["C1"], data=[[2],[3]])
        return frame.to_csv().encode()

    @property
    def ok(self):
        return True


def test_fetch_csv(monkeypatch):
    # Any arguments may be passed and mock_get() will always return our
    # mocked object, which only has the .json() method.
    def mock_get(*args, **kwargs):
        return MockResponse()

    # apply the monkeypatch for requests.get to mock_get
    monkeypatch.setattr(requests, "get", mock_get)

    result = fetch_csv("https://fakeurl", index_col=0)
    pdt.assert_frame_equal(result, pd.DataFrame(index=["A","B"], columns=["C1"], data=[[2],[3]]))
