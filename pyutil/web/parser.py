import json
import pandas as pd

from flask import Response


class HighchartsSeries(object):
    @staticmethod
    def to_json(series):
        return [[int(pd.Timestamp(t).value * 1e-6), float(value)] for t, value in series.dropna().items()]

    @staticmethod
    def parse(value):
        return pd.Series({pd.Timestamp(1e6 * int(v[0])): float(v[1]) for v in value})


def respond_pandas(object, format="json"):
    if isinstance(object, pd.DataFrame):
        if format.lower().strip() == "json":
            return Response(object.to_json(orient="table"), mimetype="application/json")

        if format.lower().strip() == "csv":
            return Response(object.to_csv(), mimetype="text/csv")

    if format.lower().strip() == "json":
        return Response(json.dumps(object), mimetype="application/json")
