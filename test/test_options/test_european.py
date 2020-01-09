import pytest

from pyutil.options.european import call, put, vanilla, OptionType


def test_call():
    # parameters
    spot = 50.0
    strike = 100.0
    r_free = 0.05  # risk-free rate
    sigma = 0.25  # annualized long-term volatility
    T = 1  # expiry in years

    c = call(spot, strike, sigma, T, r_free)
    assert c == pytest.approx(0.027352509369436617, 1e-10)

    c = vanilla(spot, strike, sigma, T, r_free, OptionType.CALL)
    assert c == pytest.approx(0.027352509369436617, 1e-10)


def test_put():
    spot = 50
    strike = 100
    r_free = 0.05
    T = 1
    sigma = 0.25

    p = put(spot, strike, sigma, T, r_free)
    assert p == pytest.approx(45.150294959440842, 1e-10)

    p = vanilla(spot, strike, sigma, T, r_free, OptionType.PUT)
    assert p == pytest.approx(45.150294959440842, 1e-10)