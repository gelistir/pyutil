import io
import pandas as pd
import requests


def fetch_csv(url, params=None, **kwargs):
    r = requests.get(url, params)
    assert r.status_code == 200, "The status code is {status}".format(status=r.status_code)
    return pd.read_csv(io.StringIO(r.content.decode()), **kwargs)


def fetch_json(url, params=None, **kwargs):
    r = requests.get(url, params)
    assert r.ok, "The status code is {status}".format(status=r.status_code)
    return pd.read_json(r.json(), **kwargs)
