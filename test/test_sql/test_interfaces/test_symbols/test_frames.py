from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.frames import Frame


class TestModels(TestCase):
    @classmethod
    def setUpClass(cls):
        ProductInterface.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=pd.Index(["A", "B"], name="wurst"), columns=["X1", "X2"])
        f = Frame(name="test", frame=x)
        pdt.assert_frame_equal(f.frame, x)

    def test_wrong_type(self):
        x = 5
        with self.assertRaises(AssertionError):
            Frame(name="test", frame=x)
