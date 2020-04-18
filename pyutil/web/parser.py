import json
import pandas as pd
from flask import Response


def respond_pandas(object, format="json"):
    if isinstance(object, pd.DataFrame):
        if format.lower().strip() == "json":
            return Response(object.to_json(orient="table"), mimetype="application/json")

        if format.lower().strip() == "csv":
            return Response(object.to_csv(), mimetype="text/csv")

    if format.lower().strip() == "json":
        return Response(json.dumps(object), mimetype="application/json")
