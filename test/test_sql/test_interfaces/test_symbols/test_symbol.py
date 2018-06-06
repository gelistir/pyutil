# import pandas as pd
# from unittest import TestCase
#
# from pyutil.sql.model.ref import Field, FieldType, DataType
# from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
#
#
# class TestSymbol(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         cls.p1 = Symbol(name="A", group=SymbolType.fixed_income)
#         cls.p2 = Symbol(name="B", group=SymbolType.equities)
#
#         cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)
#
#         cls.p1.reference[cls.f1] = "100"
#         cls.p1.reference[cls.f1] = "200"
#
#         cls.p1.upsert_ts(name="price", data={pd.Timestamp("12-11-1978"): 10.0})
#
#     def test_reference(self):
#         self.assertEqual(self.p1.reference[self.f1], 200)
#
#     def test_timeseries(self):
#         self.assertEqual(self.p1.get_timeseries("price")[pd.Timestamp("12-11-1978")], 10.0)
#
#     def test_internal(self):
#         self.assertIsNone(self.p1.internal)
#
#     def test_group(self):
#         self.assertEqual(self.p1.group.name, "fixed_income")
#
#     def test_to_html_dict(self):
#         self.p1.to_html_dict()
