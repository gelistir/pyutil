# from unittest import TestCase
#
# from pyutil.engine.portfolio import Strat, portfolios
# from pyutil.engine.symbol import Symbol, assets
#
# # An empty database requires special care, we do that here...
# from test.config import connect
#
#
# class TestMongoArchive(TestCase):
#     @classmethod
#     def setUpClas(cls):
#         connect()
#
#     def test_history_empty(self):
#         self.assertEqual(Strat.objects.count(), 0)
#         self.assertEqual(Symbol.objects.count(), 0)
#
#         a = assets()
#         p = portfolios()
#
#         self.assertTrue(a.empty)
#         self.assertTrue(p.empty)
#
#     def test_asset_builder(self):
#         with self.assertRaises(AssertionError):
#             assets(names=["a"])
#
#     def test_portfolio_builder(self):
#         with self.assertRaises(AssertionError):
#             portfolios(names=["a"])
#
#



