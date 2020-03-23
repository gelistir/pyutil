import numpy as np
import pandas.util.testing as pdt

from pyutil.portfolio.portfolio import similar, one_over_n
from test.config import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)


@pytest.fixture(scope="module")
def portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))


class TestPortfolio(object):
    def test_one_over_n(self):
        prices = pd.DataFrame(index=[1, 2, 3], columns=["A", "B"], data=[[np.nan, 2.0], [3.0, np.nan], [np.nan, 4.0]])
        portfolio = one_over_n(prices=prices)
        pdt.assert_frame_equal(portfolio.weights, pd.DataFrame(index=[1, 2, 3], columns=["A", "B"],
                                                               data=[[np.nan, 1.0], [0.5, 0.5], [0.5, 0.5]]))

    def test_from_position(self):
        prices = pd.DataFrame(index=[1, 2, 3], columns=["A", "B"], data=[[1, 1], [1.1, 1.2], [1.1, 1.4]])
        position = pd.Series(index=["A", "B"], data=[100, 200])
        p = Portfolio.fromPosition(prices=prices, position=position, cash=5)
        assert p.cash.iloc[-1] == pytest.approx(0.012658227848101222, 1e-10)

    def test_leverage(self, portfolio):
        assert portfolio.leverage["2013-07-19"] == pytest.approx(0.25505730106555635, 1e-10)
        assert portfolio.nav["2013-07-19"] == pytest.approx(0.9849066065468487, 1e-10)
        assert portfolio.cash["2015-04-22"] == pytest.approx(0.69102612448658074, 1e-10)

        # test the set...
        assert set(portfolio.assets) == {'A', 'B', 'C', 'D', 'E', 'F', 'G'}

    def test_truncate(self, portfolio):
        assert portfolio.truncate(before="2013-01-08").index[0] == pd.Timestamp("2013-01-08")

    def test_tail(self, portfolio):
        x = portfolio.tail(5)
        assert len(x.index) == 5
        assert x.index[0] == pd.Timestamp("2015-04-16")

    def test_position(self, portfolio):
        x = 1e6 * portfolio.position
        assert x["A"][pd.Timestamp("2015-04-22")] == pytest.approx(59.77214866291216, 1e-5)

    def test_mul(self, portfolio):
        pdt.assert_frame_equal(0.5 * portfolio.weights, (0.5 * portfolio).weights, check_names=False)

    def test_init_1(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
        weights = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[0.3, 0.7], [0.3, 0.7], [0.3, 0.7]])
        portfolio = Portfolio(prices=prices, weights=weights)
        assert portfolio.weights["A"][3] == pytest.approx(0.3, 1e-5)
        assert portfolio.prices["B"][3] == pytest.approx(15.0, 1e-5)

    def test_init_2(self):
        with pytest.raises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3],
                                  data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            # wrong index
            weights = pd.DataFrame(columns=["A", "B"], index=[1.5], data=[[0.3, 0.7]])
            Portfolio(prices=prices, weights=weights)

    def test_init_3(self):
        with pytest.raises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3],
                                  data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            # mismatch in columns
            weights = pd.DataFrame(columns=["C"], index=[1.5], data=[[0.3]])
            Portfolio(prices=prices, weights=weights)

    def test_mismatch_columns(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(columns=["A"]), weights=pd.DataFrame(columns=["B"]))

    def test_mismatch_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[0]), weights=pd.DataFrame(index=[1]))

    def test_monotonic_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1, 0]), weights=pd.DataFrame(index=[1, 0]))

    def test_duplicates_index(self):
        with pytest.raises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1, 1]))

    def test_series(self):
        prices = pd.DataFrame(index=[0, 1, 2], columns=["A", "B"])
        weights = pd.Series(index=["A", "B"], data=[0.5, 0.5])
        p = Portfolio(prices=prices, weights=weights)
        assert p.weights["B"][2] == 0.5

    def test_weight_current(self, portfolio):
        assert portfolio.weight_current["D"] == pytest.approx(0.022837914929098344, 1e-10)

    def test_subportfolio(self, portfolio):
        sub = portfolio.subportfolio(assets=portfolio.assets[:2])
        assert portfolio.assets[:2] == sub.assets

    def test_apply(self, portfolio):
        pdt.assert_frame_equal(portfolio.apply(lambda x: 0.5 * x).weights, 0.5 * portfolio.weights)

    def test_similar(self, portfolio):
        assert not similar(portfolio, 5)
        assert not similar(portfolio, portfolio.subportfolio(assets=["A", "B", "C"]))
        assert not similar(portfolio, portfolio.tail(100))

        p2 = Portfolio(weights=0.5*portfolio.weights, prices=portfolio.prices)
        assert not similar(portfolio, p2)
        assert similar(portfolio, portfolio)

    def test_merge(self, portfolio):
        # if old is not given
        p = Portfolio.merge(portfolio, old=None)
        assert similar(p, portfolio)

        p = Portfolio.merge(old=portfolio, new=3 * portfolio.tail(10))
        assert similar(p.tail(10), 3 * portfolio.tail(10))
        assert similar(p.head(10), portfolio.head(10))

    def test_empty_weights(self, portfolio):
        pdt.assert_frame_equal(Portfolio.fromPosition(prices=portfolio.prices).weights, 0.0*portfolio.prices)

    def test_to_frame(self, portfolio):
        frame = portfolio.to_frame(name="")
        pdt.assert_series_equal(frame["-cash"], portfolio.cash, check_names=False)
        pdt.assert_series_equal(frame["-leverage"], portfolio.leverage, check_names=False)

    def test_sector(self, portfolio):
        symbolmap = {"A": "Alternatives", "B": "Alternatives",
                     "C": "Equities", "D": "Equities",
                     "E": "Fixed Income", "F": "Fixed Income", "G": "Fixed Income"}

        frame = portfolio.sector(symbolmap=symbolmap)
        pdt.assert_series_equal(portfolio.weights[["A", "B"]].sum(axis=1), frame["Alternatives"], check_names=False)

        frame = portfolio.sector(symbolmap=symbolmap, total=True)
        pdt.assert_series_equal(portfolio.weights[["A", "B"]].sum(axis=1), frame["Alternatives"], check_names=False)

    # def test_csv(self, portfolio):
    #     portfolio.to_csv(folder="/tmp/maffay", name="peter")
    #     p = Portfolio.read_csv(folder="/tmp/maffay", name="peter")
    #     assert similar(portfolio, p)

    def test_iron_threshold(self, portfolio):
        p = portfolio.iron_threshold(threshold=0.05)
        diff = (p.weights - portfolio.weights).abs().sum(axis=1)
        rebalancing = diff[diff == 0]
        # start + rebalancing + last day
        assert len(rebalancing) == 1 + 39 + 1

    def test_iron_time(self, portfolio):
        p = portfolio.iron_time(rule="3M")
        diff = (p.weights - portfolio.weights).abs().sum(axis=1)
        rebalancing = diff[diff == 0]
        # start + rebalancing + last day
        assert len(rebalancing) == 1 + 9 + 1