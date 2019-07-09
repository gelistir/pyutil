import pandas as pd

from pyutil.sql.interfaces.ref import Field, FieldType, DataType
from test.test_sql.product import Product

import pandas.util.testing as pdt

class TestReference(object):
    def test_reference(self):
        field = Field(name="Peter", type=FieldType.dynamic, result=DataType.integer, addepar="Maffay")
        field_no = Field(name="Hans", type=FieldType.dynamic, result=DataType.integer)
        product = Product(name="A")

        product.reference[field] = "100"
        # does the conversion for me...
        assert product.reference[field] == 100
        # you can also use a string to get to the field...
        #assert product.get_reference(field="Peter") == 100

        assert product.reference.get(field) == 100


        # this field is not in the dictionary
        assert product.reference.get(field_no, 120) == 120
        assert product.reference == {field: 100}

        product.reference[field] = "120"
        assert product.reference[field] == 120

        assert DataType.integer.value == "integer"
        assert DataType.integer("210") == 210
        assert str(field) == "(Peter)"
        assert field.addepar == "Maffay"

    def test_eq(self):
        f1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        f2 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        assert f1 == f2
        assert hash(f1) == hash(f2)
        assert f1.type == FieldType.dynamic

    def test_lt(self):
        f1 = Field(name="A")
        f2 = Field(name="B")
        assert f1 < f2

    def test_reference_series(self):
        p = Product(name="A")
        field1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.integer)
        field2 = Field(name="Hans", type=FieldType.dynamic, result=DataType.string)

        p.reference[field1] = "120"
        p.reference[field2] = "wurst"

        #print(pd.Series(dict(p.reference)))

        pdt.assert_series_equal(p.reference_series, pd.Series({"Hans": "wurst", "Peter": 120}))

    def test_reference_frame(self):
        p = Product(name="A")
        field1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.integer)
        field2 = Field(name="Hans", type=FieldType.dynamic, result=DataType.string)

        p.reference[field1] = "120"
        p.reference[field2] = "wurst"

        f = Product.reference_frame(products=[p])

        framex = pd.DataFrame(index=[field2.name, field1.name], columns=[p], data=["wurst", 120]).transpose()
        framex.index.name = "product"

        pdt.assert_frame_equal(f, framex)
