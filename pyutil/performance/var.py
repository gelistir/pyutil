import numpy as np


def __tail(losses, alpha=0.99):
    return np.sort(losses)[int(losses.size * alpha):]


def value_at_risk(nav, alpha=0.99):
    """
    Compute the alpha Value at Risk (VaR) for nav series

    :param nav: the nav series
    :param alpha: the parameter alpha

    :return: the smallest loss in the (1-alpha) fraction of biggest losses, e.g. smallest loss in the tail
    """
    losses = nav.pct_change().dropna()*(-1)
    return __tail(losses, alpha)[0]


def conditional_value_at_risk(nav, alpha=0.99):
    """
    Compute the alpha Conditional Value at Risk (CVaR) for nav series

    :param nav: the nav series
    :param alpha: the parameter alpha

    :return: the mean of the (1-alpha) fraction of biggest losses, e.g. the mean of the tail
    """
    losses = nav.pct_change().dropna()*(-1)
    return np.mean(__tail(losses, alpha))


class VaR(object):
    def __init__(self, series, alpha = 0.99):
        self.__series = series.dropna()
        self.__alpha = alpha

    @property
    def __losses(self):
        return self.__series.pct_change().dropna() * (-1)

    @property
    def __tail(self):
        losses = self.__losses
        return np.sort(losses.values)[int(losses.shape[0] * self.__alpha):]

    @property
    def cvar(self):
        return self.__tail.mean()

    @property
    def var(self):
        return self.__tail[0]