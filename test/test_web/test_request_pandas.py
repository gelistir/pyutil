from unittest import TestCase

import pandas as pd

from pyutil.web.request_pandas import RequestPandas



class TestRequestPandas(TestCase):
    def test_link(self):
        #data = {"Maffay": pd.Series(index=[0,1], data=[2,3]).to_json(), "Peter": pd.DataFrame().to_json(orient="split")}

        r = RequestPandas()
        r["Maffay"] = pd.Series(index=[0,1], data=[2,3])
        r["Peter"] = pd.DataFrame(index=[0,1], columns=["A", "B"])

        print(r.get_series("Maffay"))
        print(r.get_frame("Peter"))
        print(r.json())

