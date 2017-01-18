from unittest import TestCase

import pandas as pd

from pyutil.web.request_pandas import RequestPandas
import pandas.util.testing as pdt

class TestRequestPandas(TestCase):
    def setUp(self):
        self.a = pd.Series(index=[0,1], data=[2,3])
        self.b = pd.DataFrame(index=[0,1], columns=["A", "B"], data=0)

    def test_build(self):
        r = RequestPandas()
        r["Maffay"] = self.a
        r["Peter"] = self.b

        pdt.assert_series_equal(self.a, r.get_series("Maffay"))
        pdt.assert_frame_equal(self.b, r.get_frame("Peter"))
        self.assertEqual(r.json(), '{"Maffay": "{\\"0\\":2,\\"1\\":3}", "Peter": "{\\"columns\\":[\\"A\\",\\"B\\"],\\"index\\":[0,1],\\"data\\":[[0,0],[0,0]]}"}')

    def test_json_str(self):
        r = RequestPandas(json_str='{"Maffay": "{\\"0\\":2,\\"1\\":3}", "Peter": "{\\"columns\\":[\\"A\\",\\"B\\"],\\"index\\":[0,1],\\"data\\":[[0,0],[0,0]]}"}')
        pdt.assert_series_equal(r.get_series("Maffay"), self.a)
        pdt.assert_frame_equal(r.get_frame("Peter"), self.b)

