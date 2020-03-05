import warnings
import numpy as np
import pandas as pd

from pyutil.performance.drawdown import drawdown
from pyutil.performance.month import monthlytable
from pyutil.performance.return_series import performance

warnings.simplefilter(action='ignore', category=FutureWarning)

import beakerx as bx


def container():
    layout_manager = bx.TabbedOutputContainerLayoutManager()
    layout_manager.setBorderDisplayed(False)
    container = bx.OutputContainer()
    container.setLayoutManager(layout_manager)
    return container


def nav_curve(nav, name=None, showLegend=True, **kwargs):
    p = bx.TimePlot(xLabel="Time", yLabel="NAV", title=name, showLegend=showLegend, **kwargs)

    p.getYAxes()[0].setBound(0, nav.max() + 0.1)
    y_axis = bx.YAxis(label="Drawdown", upperMargin=2)
    y_axis.setBound(-0.1, 1.1)
    p.add(y_axis)

    p.add(bx.Line(nav, displayName="NAV"))
    p.add(bx.Area(drawdown(nav), displayName="Drawdown", yAxis="Drawdown"))

    return p


def display_monthtable(nav):
    return pandas_display(frame=100 * monthlytable(nav.pct_change().fillna(0.0)))


def display_performance(nav):
    perf = nav.apply(performance)
    perf.drop(index=["First at", "Last at"], inplace=True)
    perf = perf.applymap(lambda x: float(x))
    return pandas_display(frame=perf)


def pandas_display(frame, columns=None):
    display = bx.TableDisplay(frame)
    display.setStringFormatForType(bx.ColumnType.Double, bx.TableDisplayStringFormat.getDecimalFormat(2, 2))

    # either use some or all columns
    columns = columns or frame.keys()

    for key in columns:
        display.setAlignmentProviderForColumn(key, bx.TableDisplayAlignmentProvider.RIGHT_ALIGNMENT)

    # get to indices...
    columns = [display.chart.columnNames.index(column) for column in columns]

    for row, v in enumerate(display.values):
        for column in columns:
            try:
                if np.isnan(float(v[column])):
                    display.values[row][column] = ''
            except ValueError:
                pass

    display.sendModel()
    return display
