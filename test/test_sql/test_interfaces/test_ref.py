from pyutil.sql.interfaces.ref import Field, FieldType, DataType
from test.test_sql.product import Product


class TestReference(object):
    def test_reference(self):
        field = Field(name="Peter", type=FieldType.dynamic, result=DataType.integer)
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

    def test_eq(self):
        f1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        f2 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        assert f1 == f2
        assert hash(f1) == hash(f2)
