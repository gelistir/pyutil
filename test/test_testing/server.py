import pandas as pd
from flask import Flask, request
from pyutil.web.parser import respond_pandas

frame = pd.DataFrame(index=["A"], columns=["B"], data=[[3.0]])


app = Flask(__name__)


@app.route("/hello")
def hello():
    return "Hello World!"


@app.route("/csv")
def csv():
    return respond_pandas(object=frame, format="csv")


@app.route("/json")
def json():
    return respond_pandas(object=frame, format="json")


@app.route("/post", methods=("POST",))
def post_hello():
    assert request.method == "POST"
    return "Hello Thomas!"


