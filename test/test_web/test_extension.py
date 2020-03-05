import pytest
from flask import Flask

from pyutil.web.extension import Extension, InvalidSettingsError


class Maffay(Extension):
    def __init__(self, name, app=None, config=None):
        super().__init__(name, app=app, config=config)

    def create(self, config):
        # Validate that the config is a dict
        if config is None or not isinstance(config, dict):
            raise InvalidSettingsError('Invalid application configuration')

        # Otherwise, return a single connection
        return config["PETER"]


def test_flask_extension():
    app = Flask(__name__)
    app.config["PETER"] = "MAFFAY"
    with app.app_context():
        maffay = Maffay(name="MAFFAY")
        maffay.init_app(app)
        assert maffay.ext == "MAFFAY"

def test_init_1():
    app = Flask(__name__)
    app.config["PETER"] = "MAFFAY"
    m = Maffay(name="MAFFAY", app=app)
    assert isinstance(app.extensions["MAFFAY"][m], dict)

def test_init_2():
    with pytest.raises(Exception):
        Maffay(name="MAFFAY", app="APP")

def test_init_3():
    with pytest.raises(Exception):
        app = Flask(__name__)
        app.config["PETER"] = "MAFFAY"
        with app.app_context():
            maffay = Maffay(name="MAFFAY")
            maffay.init_app(app)
            maffay.init_app(app)


def test_abstract():
    m = Extension(name="MAFFAY")
    with pytest.raises(NotImplementedError):
        m.create(config={})
