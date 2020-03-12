import numpy as np
from scipy.stats import norm
#from enum import Enum


#class OptionType(Enum):
#    CALL = "CALL"
#    PUT = "PUT"

#option_types = {x.value : x for x in OptionType}


def call(spot, strike, sigma, T, r_free=0):
    # spot: spot price
    # strike: strike price
    # sigma: volatility of underlying asset
    # T: time to maturity
    # r_free: interest rate

    d1 = (np.log(spot / strike) + r_free * T + 0.5 * sigma ** 2 * T) / (sigma * np.sqrt(T))
    d2 = (np.log(spot / strike) + r_free * T - 0.5 * sigma ** 2 * T) / (sigma * np.sqrt(T))
    return spot * norm.cdf(d1) - strike * np.exp(-r_free * T) * norm.cdf(d2)


def put(spot, strike, sigma, T, r_free=0):
    # spot: spot price
    # strike: strike price
    # sigma: volatility of underlying asset
    # T: time to maturity
    # r_free: interest rate

    d1 = (np.log(spot / strike) + (r_free + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = (np.log(spot / strike) + (r_free - 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))

    return (strike * np.exp(-r_free * T) * norm.cdf(-d2) - spot * norm.cdf(-d1))


def vanilla(spot, strike, sigma, T, r_free=0, option="CALL"):
    if option == "CALL":
        return call(spot, strike, sigma, T, r_free)

    if option == "PUT":
        return put(spot, strike, sigma, T, r_free)



