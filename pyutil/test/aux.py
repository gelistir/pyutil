import os
import pandas as pd
from functools import partial


def post(client, data, url):
    response = client.post(url, data=data)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


def get(client, url):
    response = client.get(url)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


def resource_folder(folder):
    def __resource(name, folder):
        return os.path.join(folder, "resources", name)

    return partial(__resource, folder=folder)


def read_frame(file, header=0, parse_dates=True, index_col=0, **kwargs):
    return pd.read_csv(file, index_col=index_col, header=header, parse_dates=parse_dates, **kwargs)


def read_series(file, parse_dates=True, index_col=0, cname=None, **kwargs):
    return pd.read_csv(file, index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates,
                       names=[cname], **kwargs)
