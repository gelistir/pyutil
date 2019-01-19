import pandas.util.testing as pdt

from pyutil.sql.interfaces.whoosh import Whoosh
from test.config import read


class TestWhoosh(object):
    def test_whoosh(self):
        w1 = Whoosh(title="A", content="AA", path="aaa", group="GA")
        w2 = Whoosh(title="B", content="BB", path="bbb", group="GB")

        w = read("whoosh.csv")

        pdt.assert_frame_equal(w, Whoosh.frame(rows=[w1,w2]), check_dtype=False)

        assert str(w1) == "Whoosh(title=A)"
