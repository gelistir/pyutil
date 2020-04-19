from pyutil.config.yaml import read_config
from test.config import resource


def test_yaml():
    d = read_config(resource("config.yml"))
    assert "Clients" in d.keys()
