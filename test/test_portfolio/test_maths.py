from pyutil.portfolio.maths import xround, buy_or_sell
from unittest import TestCase


class TestMaths(TestCase):
    def test_pos_scalar(self):
        self.assertEqual(xround(1421.2, 6), 1421)
        self.assertEqual(xround(1421.2, 3), 1420)
        self.assertEqual(xround(1421.2, 2), 1400)
        self.assertEqual(xround(1421.2, 1), 1000)

    def test_neg_scalar(self):
        self.assertEqual(xround(-1421.2, 6), -1422)
        self.assertEqual(xround(-1421.2, 3), -1430)
        self.assertEqual(xround(-1421.2, 2), -1500)
        self.assertEqual(xround(-1421.2, 1), -2000)

    def test_array(self):
        self.assertEqual(xround([-1421.2, 1421.2], 6)[0], -1422)
        self.assertEqual(xround([-1421.2, 1421.2], 6)[1], 1421)
        self.assertEqual(xround([-1421.2, 1421.2], 3)[0], -1430)
        self.assertEqual(xround([-1421.2, 1421.2], 3)[1], 1420)
        self.assertEqual(xround([-1421.2, 1421.2], 2)[0], -1500)
        self.assertEqual(xround([-1421.2, 1421.2], 2)[1], 1400)
        self.assertEqual(xround([-1421.2, 1421.2], 1)[0], -2000)
        self.assertEqual(xround([-1421.2, 1421.2], 1)[1],  1000)

    def test_buy(self):
        self.assertEqual(buy_or_sell(5.0), "Buy")
        self.assertEqual(buy_or_sell(-5.0), "Sell")
