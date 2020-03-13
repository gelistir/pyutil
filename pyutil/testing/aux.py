from io import StringIO

import pandas as pd


def post(client, data, url):
    response = client.post(url, data=data)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response


def get(client, url):
    response = client.get(url)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response


def response2csv(response, **kwargs):
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return pd.read_csv(StringIO(response.data.decode()), **kwargs)


def response2json(response, **kwargs):
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return pd.read_json(response.data, **kwargs)
