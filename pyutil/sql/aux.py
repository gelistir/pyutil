import pandas as pd

from pyutil.sql.models import Symbol, Timeseries, _TimeseriesData


# aux. function to access Symbols by name....
def asset(session, name):
    return session.query(Symbol).filter(Symbol.bloomberg_symbol == name).first()


def history(session, field="PX_LAST"):
    x = session.query(_TimeseriesData.date, Symbol.bloomberg_symbol, _TimeseriesData.value).join(Timeseries).join(
        Symbol).filter(Timeseries.name == field)
    a = pd.DataFrame.from_records(data=[(date, asset, price) for (date, asset, price) in x], index=["Date", "Asset"],
                                  columns=["Date", "Asset", "Price"])
    return a["Price"].unstack()


def reference(session):
    x = pd.DataFrame({symbol.bloomberg_symbol: symbol.reference for symbol in session.query(Symbol)}).transpose()
    x.index.name = "Asset"
    return x
