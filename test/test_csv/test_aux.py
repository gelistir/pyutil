from pyutil.csv.aux import frame2file
from unittest import TestCase
import os
import pandas as pd
import numpy as np


class TestCsv(TestCase):
    def testFile2Frame(self):
        frame = pd.DataFrame(np.random.randn(5, 5))
        file = frame2file(frame)
        assert os.path.exists(file.name)
        os.remove(file.name)
        assert not os.path.exists(file.name)
