import os
from pyutil.web.application import create_server

os.environ["APPLICATION_SETTINGS"] = "/pyutil/test/resources/settings.cfg"


class MockExtension(object):
    def init_app(self, server):
        pass


def test_application_extensions():
    assert create_server(extensions=[MockExtension()])


def test_application():
    assert create_server(extensions=[])
