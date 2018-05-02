from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.interfaces.symbols.frames import Frame


class TestModels(TestCase):
    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
        x.index.names = ["index"]
        f = Frame(name="test", frame=x)
        pdt.assert_frame_equal(f.frame, x)
