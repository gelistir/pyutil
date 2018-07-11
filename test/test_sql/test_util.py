from unittest import TestCase

import pandas as pd
from pyutil.sql.util import parse


class TestUtil(TestCase):
    def test_parse(self):
        self.assertEqual(parse("2.0", "integer"), 2)
        self.assertEqual(parse("2.0", "float"), 2.0)
        self.assertEqual(parse("2.0", "string"), "2.0")
        self.assertEqual(parse("0.2", "percentage"), 0.2)
        self.assertEqual(parse("1527724800000", "date"), pd.Timestamp("2018-05-31"))

    def test_none(self):
        self.assertIsNone(parse(x=None))
