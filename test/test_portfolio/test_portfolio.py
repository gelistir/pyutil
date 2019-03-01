import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.portfolio.portfolio import Portfolio, similar
from test.config import test_portfolio, read


@pytest.fixture(scope="module")
def portfolio():
    return test_portfolio()

@pytest.fixture(scope="module")
def sector_weights():
    return read("sector_weights.csv", parse_dates=True)

class TestPortfolio(object):
    def test_leverage(self, portfolio):
        assert portfolio.leverage["2013-07-19"] == pytest.approx(0.25505730106555635, 1e-10)
        assert portfolio.nav["2013-07-19"] == pytest.approx(0.9849066065468487, 1e-10)
        assert portfolio.cash["2015-04-22"] == pytest.approx(0.69102612448658074, 1e-10)

        # test the set...
        assert set(portfolio.assets) == {'A', 'B', 'C', 'D', 'E', 'F', 'G'}

    def test_truncate(self, portfolio):
        assert portfolio.truncate(before="2013-01-08").index[0] == pd.Timestamp("2013-01-08")


    def test_top_flop(self, portfolio):
        xx = portfolio.top_flop(day_final=pd.Timestamp("2014-12-31"))
        assert xx.ytd.top.values[0] == pytest.approx(0.024480763822820828, 1e-10)
        assert xx.mtd.flop.values[0] == pytest.approx(-0.0040598440397091595, 1e-10)


    def test_tail(sel, portfolio):
        x = portfolio.tail(5)
        assert len(x.index) == 5
        assert x.index[0] == pd.Timestamp("2015-04-16")

    def test_sector_weights(self, portfolio, sector_weights):
        portfolio.symbolmap = {"A": "A", "B": "A", "C": "B", "D": "B", "E": "C", "F": "C", "G": "C"}

        x = portfolio.sector_weights(total=True)

        pdt.assert_frame_equal(x.tail(10), sector_weights)

        x = portfolio.sector_weights_final(total=True)

        pdt.assert_series_equal(x, sector_weights.iloc[-1])


    def test_position(self, portfolio):
        x = 1e6 * portfolio.position
        assert x["A"][pd.Timestamp("2015-04-22")] == pytest.approx(60.191699988670969, 1e-5)

    def test_build_portfolio(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3],
                              data=[[1000.0, 1000.0], [1500.0, 1500.0], [2000.0, 2000.0]])
        weights = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[0.25, 0.25], [0.25, 0.25], [0.25, 0.25]])

        p = Portfolio(prices=prices, weights=weights)
        pdt.assert_frame_equal(prices, p.prices)

        assert p.position["A"][2] == pytest.approx(0.00020833333333333335, 1e-10)

    def test_mul(self, portfolio):
        #print(2 * portfolio.weights)
        #print((2 * portfolio).weights)
        pdt.assert_frame_equal(2 * portfolio.weights, (2 * portfolio).weights, check_names=False)

    def test_iron_threshold(self, portfolio):
        p1 = portfolio.truncate(before="2015-01-01").iron_threshold(threshold=0.05)
        assert len(p1.trading_days) == 5

    def test_iron_time(self, portfolio):
        p2 = portfolio.truncate(before="2014-07-01").iron_time(rule="3M")
        assert len(p2.trading_days) == 4

    def test_init_1(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
        weights = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[0.3, 0.7], [0.3, 0.7], [0.3, 0.7]])
        portfolio = Portfolio(prices=prices, weights=weights)
        assert 0.3 == pytest.approx(portfolio.weights["A"][3], 1e-5)
        assert 15.0 == pytest.approx(portfolio.prices["B"][3], 1e-5)

    def test_init_2(self):
        with pytest.raises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            weights = pd.DataFrame(columns=["A", "B"], index=[1.5], data=[[0.3, 0.7]])
            Portfolio(prices=prices, weights=weights)

    def test_init_3(self):
        with pytest.raises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            weights = pd.DataFrame(columns=["C"], index=[1.5], data=[[0.3]])
            Portfolio(prices=prices, weights=weights)

    def test_state(self, portfolio):
        portfolio.state.to_csv("state2.csv")
        pdt.assert_frame_equal(portfolio.state, read("state2.csv", squeeze=False, header=0))

    def test_mismatch_columns(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(columns=["A"]), weights=pd.DataFrame(columns=["B"]))

    def test_mismatch_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[0]), weights=pd.DataFrame(index=[1]))

    def test_monotonic_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1,0]), weights=pd.DataFrame(index=[1,0]))

    def test_duplicates_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1, 1]))

    def test_series(self):
        prices=pd.DataFrame(index=[0,1,2],columns=["A","B"])
        weights=pd.Series(index=["A","B"],data=[1.0,1.0])
        p=Portfolio(prices=prices, weights=weights)
        assert p.weights["B"][2] == 1

    def test_gap(self):
        prices=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=100)
        weights=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=[1,np.nan,1,1])
        with pytest.raises(AssertionError):
            Portfolio(prices=prices, weights=weights)

    def test_weight_current(self, portfolio):
        assert portfolio.weight_current["D"] == pytest.approx(0.022837914929098344, 1e-10)

    def test_subportfolio(self, portfolio):
        sub = portfolio.subportfolio(assets=portfolio.assets[:2])
        assert portfolio.assets[:2] == sub.assets

    # def test_snapshot(self, portfolio):
    #     x = portfolio.snapshot(n=5)
    #     assert x["Year-to-Date"]["B"] == pytest.approx(0.01615087992272124, 1e-10)

    def test_apply(self, portfolio):
        w = portfolio.apply(lambda x: 2*x)
        pdt.assert_frame_equal(w.weights, 2*portfolio.weights)

    def test_similar(self, portfolio):
        assert not similar(portfolio, 5)
        assert not similar(portfolio, portfolio.subportfolio(assets=["A","B","C"]))
        assert not similar(portfolio, portfolio.tail(100))

        p2 = Portfolio(weights=2*portfolio.weights, prices=portfolio.prices)
        assert not similar(portfolio, p2)
        assert similar(portfolio, portfolio)