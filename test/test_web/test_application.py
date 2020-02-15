import os

from pyutil.web.application import create_server


def test_application():
    os.environ["APPLICATION_SETTINGS"] = "/pyutil/test/resources/settings.cfg"
    server = create_server()

    with server.test_request_context():
        # register the blueprint
        pass

    assert server

