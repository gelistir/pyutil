import numpy as np


def __tail(losses, alpha=0.99):
    return np.sort(losses)[int(losses.size*alpha):]


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


