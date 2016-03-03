import numpy as np
import pandas as pd
from unittest import TestCase


class TestCsv(TestCase):
    def test_file(self):
        frame = pd.DataFrame(np.random.randn(5, 5))

        from pyutil.config import mail
        m = mail()
        m.toAdr = "thomas.schmelzer@gmail.com"
        m.fromAdr = "monitor@lobnek.com"
        m.attach_stream("hans.csv", frame.to_csv())
        m.send(text="test")