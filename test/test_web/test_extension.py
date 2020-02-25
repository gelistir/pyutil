from flask import Flask

from pyutil.web.extension import Extension, InvalidSettingsError


class Maffay(Extension):
    def __init__(self, name):
        super().__init__(name)

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



