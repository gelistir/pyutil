import pandas as pd
import pytest

from pyutil.mongo.mongo import mongo_client
from test.test_sql.maffay import Maffay
import pandas.util.testing as pdt


@pytest.fixture()
def product():
    product = Maffay(name="A")
    product.reference["aaa"] = "A"
    product.reference["bbb"] = "Z"
    return product


class TestReference(object):
    def test_get_item(self, product):
        assert product.name == "A"
        assert product.reference["NoNoNo"] is None
        assert product.reference["bbb"] == "Z"

    def test_ref_series(self, product):
        assert product.reference.keys() == {"aaa", "bbb"}

    def test_ref_frame_1(self, product):
        f1 = Maffay.reference_frame(products=[product])
        f2 = pd.DataFrame(index=[product], columns=["aaa", "bbb"], data=[["A", "Z"]])
        pdt.assert_frame_equal(f1, f2, check_names=False)
        assert f1.index.name == "maffay"

    def test_get(self, product):
        assert product.reference.get(item="NoNoNo", default=5) == 5

    def test_items(self, product):
        assert {k: v for k, v in product.reference.items()} == {"aaa": "A", "bbb": "Z"}
