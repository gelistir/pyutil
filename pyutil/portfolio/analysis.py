import pandas as pd
from pyutil.performance.summary import fromNav
from pyutil.portfolio.format import fint, fdouble, percentage


def __last(frame, datefmt=None):
    frame = frame.sort_index(axis=1, ascending=False)
    if datefmt:
        frame = frame.rename(columns=lambda x: x.strftime(datefmt))
    frame["total"] = (frame + 1).prod(axis=1) - 1
    frame.index.name = "Portfolio"
    return frame


#def percentage(x):
#    return "{0:.2f}%".format(float(x)).replace("nan%", "")

#def fdouble(x):
#    return "{0:.2f}".format(float(x)).replace("nan", "")

#def fint(x):
#    try:
#        return "{:d}".format(int(x))
#    except:
#        return ""

def nav(portfolios, f=lambda x: x) -> pd.DataFrame:
    assert isinstance(portfolios, dict)
    return pd.DataFrame({name: f(portfolio.nav) for name, portfolio in portfolios.items()})


def mtd(portfolios):
    frame = 100*nav(portfolios=portfolios, f=lambda x: fromNav(x).mtd_series).transpose()
    return __last(frame, datefmt="%b %d")#.applymap(percentage)


def ytd(portfolios):
    frame = 100*nav(portfolios=portfolios, f=lambda x: fromNav(x).ytd_series).transpose()
    return __last(frame, datefmt="%m")#.applymap(percentage)


def recent(portfolios, n=15):
    # define the function
    frame = 100*nav(portfolios=portfolios, f=lambda x: fromNav(x).recent(n)).tail(n).transpose()
    return __last(frame, datefmt="%b %d")#.applymap(percentage)


def sector(portfolios, symbols, total=False):
    assert isinstance(portfolios, dict)
    frame = 100*pd.DataFrame(
        {name: portfolio.sector(symbols, total=total).iloc[-1] for name, portfolio in portfolios.items()}).transpose()
    frame.index.name = "Portfolio"
    return frame#.applymap(percentage)


def performance(portfolios, **kwargs):
    perf = nav(portfolios=portfolios, f=lambda x: fromNav(x).summary(**kwargs))

    perf.loc[["Kurtosis", "Current Nav", "Max Nav", "Annua Sharpe Ratio (r_f = 0)", "Calmar Ratio (3Y)"]] =\
        perf.loc[["Kurtosis", "Current Nav", "Max Nav", "Annua Sharpe Ratio (r_f = 0)", "Calmar Ratio (3Y)"]].applymap(fdouble)

    perf.loc[["# Events", "# Events per year", "# Positive Events", "# Negative Events"]] = \
        perf.loc[["# Events", "# Events per year", "# Positive Events", "# Negative Events"]].applymap(fint)

    perf.loc[["Return", "Annua Return", "Annua Volatility", "Max Drawdown", "Max % return", "Min % return", "MTD", "YTD", "Current Drawdown", "Value at Risk (alpha = 95)", "Conditional Value at Risk (alpha = 95)"]] = \
        perf.loc[
            ["Return", "Annua Return", "Annua Volatility", "Max Drawdown", "Max % return", "Min % return", "MTD", "YTD",
             "Current Drawdown", "Value at Risk (alpha = 95)", "Conditional Value at Risk (alpha = 95)"]].applymap(percentage)

    return perf

    #perf.drop(index=["First at", "Last at"], inplace=True)
    #return perf.applymap(lambda x: float(x))

def drawdown(portfolios):
    return nav(portfolios=portfolios, f=lambda x: fromNav(x).drawdown)


def ewm_volatility(portfolios, **kwargs):
    return nav(portfolios=portfolios, f=lambda x: fromNav(x).ewm_volatility(**kwargs))


def period(portfolios, before=None, after=None, adjust=True):
    return nav(portfolios=portfolios, f=lambda x: fromNav(x).truncate(before=before, after=after, adjust=adjust))


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
