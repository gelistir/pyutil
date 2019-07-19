import pandas as pd


def __last(frame, datefmt=None):
    frame = frame.sort_index(axis=1, ascending=False)
    if datefmt:
        frame = frame.rename(columns=lambda x: x.strftime(datefmt))
    frame["total"] = (frame + 1).prod(axis=1) - 1
    frame.index.name = "Portfolio"
    return frame


def nav(portfolios, f=lambda x: x) -> pd.DataFrame:
    assert isinstance(portfolios, dict)
    return pd.DataFrame({name: f(portfolio.nav) for name, portfolio in portfolios.items()})


def mtd(portfolios):
    frame = nav(portfolios=portfolios, f=lambda x: x.mtd_series).transpose()
    return __last(frame, datefmt="%b %d")


def ytd(portfolios):
    frame = nav(portfolios=portfolios, f=lambda x: x.ytd_series).transpose()
    return __last(frame, datefmt="%m")


def recent(portfolios, n=15):
    # define the function
    frame = nav(portfolios=portfolios, f=lambda x: x.recent(n)).tail(n).transpose()
    return __last(frame, datefmt="%b %d")


def sector(portfolios, symbols, total=False):
    assert isinstance(portfolios, dict)
    frame = pd.DataFrame(
        {name: portfolio.sector(symbols, total=total).iloc[-1] for name, portfolio in portfolios.items()}).transpose()
    frame.index.name = "Portfolio"
    return frame


def performance(portfolios, **kwargs):
    return nav(portfolios=portfolios, f=lambda x: x.summary_format(**kwargs))


def drawdown(portfolios):
    return nav(portfolios=portfolios, f=lambda x: x.drawdown)


def ewm_volatility(portfolios, **kwargs):
    return nav(portfolios=portfolios, f=lambda x: x.ewm_volatility(**kwargs))


def period(portfolios, before=None, after=None, adjust=True):
    return nav(portfolios=portfolios, f=lambda x: x.truncate(before=before, after=after, adjust=adjust))


def monthlytable(portfolios):
    assert isinstance(portfolios, dict)
    return {name: portfolio.nav.monthlytable for name, portfolio in portfolios.items()}

def drawdown_periods(portfolios):
    assert isinstance(portfolios, dict)
    return {name: portfolio.nav.drawdown_periods for name, portfolio in portfolios.items()}
    #nav_period = nav.truncate(before=start, after=end).apply(adjust)
    #o.addItem(display_performance(nav_period), "Performance")
    #plot_entire = SimpleTimePlot(nav, nav.keys())

    #if start and end:
    #    plot_entire.add(ConstantBand(x=[start, end]))
    #else:
    #    if days:
    #        for day in days:
    #            plot_entire.add(ConstantLine(x=day))

    #o.addItem(plot_entire, "History")

    #if start and end:
    #    plot = SimpleTimePlot(nav_period, nav_period.keys())  # , "Period")
    #    if days:
    #        for day in days:
    #            plot.add(ConstantLine(x=day))
    #    o.addItem(plot, "Period")

    #return o
