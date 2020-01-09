import pytest
from flask import Flask, request
from pyutil.testing.aux import get, post

app = Flask(__name__)


@app.route("/hello")
def hello():
    return "Hello World!"


@app.route("/post", methods=("POST",))
def post_hello():
    assert request.method == "POST"
    return "Hello Thomas!"


@pytest.fixture(scope="module")
def client():
    app.config['TESTING'] = True
    yield app.test_client()


def test_get(client):
    get(client, url="/hello")


def test_post(client):
    post(client, url="/post", data={})
