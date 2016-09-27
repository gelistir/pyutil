import pandas as pd
import numpy as np


class PrettyPandas(object):
    """Pretty pandas dataframe Styles.

    subset of the package by H. Hammond, see https://github.com/HHammond/PrettyPandas

    Parameters
    ----------
    :param data: Series or DataFrame

    :param summary_rows:
        list of single-row dataframes to be appended as a summary
    :param summary_cols:
        list of single-row dataframes to be appended as a summary
    """

    def __init__(self, data, summary_rows=None, summary_cols=None):
        self.summary_rows = summary_rows or []
        self.summary_cols = summary_cols or []
        self.data = data

    def summary(self, func=np.sum, title='Total', axis=0, **kwargs):
        """Add multiple summary rows or columns to the dataframe.
        Parameters
        ----------
        :param func: Iterable of functions to be used for a summary.
        :param titles: Iterable of titles in the same order as the functions.
        :param axis:
            Same as numpy and pandas axis argument. A value of None will cause
            the summary to be applied to both rows and columns.
        :param kwargs: Keyword arguments passed to all the functions.
        The results of summary can be chained together.
        """
        return self.multi_summary([func], [title], axis, **kwargs)

    def multi_summary(self, funcs, titles, axis=0, **kwargs):
        """Add multiple summary rows or columns to the dataframe.
        Parameters
        ----------
        :param funcs: Iterable of functions to be used for a summary.
        :param titles: Iterable of titles in the same order as the functions.
        :param axis:
            Same as numpy and pandas axis argument. A value of None will cause
            the summary to be applied to both rows and columns.
        :param kwargs: Keyword arguments passed to all the functions.
        """
        if axis is None:
            return self.multi_summary(funcs, titles, axis=0, **kwargs)\
                       .multi_summary(funcs, titles, axis=1, **kwargs)

        output = [self.data.apply(f, axis=axis, **kwargs).to_frame(t)
                  for f, t in zip(funcs, titles)]

        if axis == 0:
            self.summary_rows += [row.T for row in output]
        elif axis == 1:
            self.summary_cols += output
        else:
            ValueError("Invalid axis selected. Can only use 0, 1, or None.")

        return self

    def total(self, title="Total", **kwargs):
        """Add a total summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.sum``.
        """
        return self.summary(np.sum, title, **kwargs)

    def average(self, title="Average", **kwargs):
        """Add a mean summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.mean``.
        """
        return self.summary(np.mean, title, **kwargs)

    def median(self, title="Median", **kwargs):
        """Add a median summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.median``.
        """
        return self.summary(np.median, title, **kwargs)

    def max(self, title="Maximum", **kwargs):
        """Add a maximum summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.max``.
        """
        return self.summary(np.max, title, **kwargs)

    def min(self, title="Minimum", **kwargs):
        """Add a minimum summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.min``.
        """
        return self.summary(np.min, title, **kwargs)

    def std(self, title="StdDev", **kwargs):
        """Add a minimum summary to this table.
        :param title: Title to be displayed.
        :param kwargs: Keyword arguments passed to ``numpy.min``.
        """
        return self.summary(np.std, title, **kwargs)

    @property
    def frame(self):
        """Add all summary rows and columns."""
        frame = self.data.copy()

        summary_colnames = [series.columns[0] for series in self.summary_cols]

        frame = pd.concat([frame] + self.summary_cols, axis=1, ignore_index=False)
        frame = pd.concat([frame] + self.summary_rows, axis=0, ignore_index=False)

        # Sort column names
        return frame[list(self.data.columns) + summary_colnames]

    def __repr__(self):
        return self.frame.to_string()