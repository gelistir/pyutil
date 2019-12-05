import os

from pyutil.config.config import configuration


class TestConfig(object):
    def __init__(self):
        os.environ["wurst"] = "settings.cfg"
        c = configuration(envvar="wurst")
        assert c["MAILGUNAPI"] == 1
