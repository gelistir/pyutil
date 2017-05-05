from unittest import TestCase
from mongoengine import connect

from pyutil.engine.symbol import Symbol, asset_builder
from pyutil.mongo.asset import Asset



class TestSymbol(TestCase):
    @classmethod
    def setUpClass(cls):
        connect(db="testSymbol", host="mongo", port=27017, alias="default")

        # Create a text-based post
        sym1 = Symbol(name="XYZ", internal="XYZ internal", group="A")
        sym1.save()

        sym2 = Symbol(name="aaa", internal="aaa internal", group="B")
        sym2.save()

    def test_pos(self):
        for asset in Symbol.objects:
            print(asset)

    def test_count(self):
        assert Symbol.objects.count()==2

    def test_builder(self):
        x = asset_builder(name="aaa")
        print(x)
        assert isinstance(x, Asset)

