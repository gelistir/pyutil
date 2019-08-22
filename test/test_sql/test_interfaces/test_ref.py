import pytest

from pyutil.sql.interfaces.symbols.symbol import Symbol


@pytest.fixture()
def product():
    product = Symbol(name="A")
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

    def test_get(self, product):
        assert product.reference.get(item="NoNoNo", default=5) == 5

    def test_items(self, product):
        assert {k: v for k, v in product.reference.items()} == {"aaa": "A", "bbb": "Z"}
