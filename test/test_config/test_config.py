import os

from pyutil.config.config import configuration, Config


class TestConfig(object):
    def test_a(self):
        os.environ["wurst"] = "/pyutil/test/test_config/settings.cfg"
        c = configuration(envvar="wurst")
        assert c["MAILGUNAPI"] == 1

    def test_b(self):
        c = Config(root_path=".")
        os.environ["wurst"] = "/pyutil/test/test_config/settings.cfg"
        c.from_envvar(variable_name="wurst")
        assert c["MAILGUNAPI"] == 1