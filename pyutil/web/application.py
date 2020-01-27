from flask import Flask


def create_server(name=__name__, static_folder="static", template_folder="templates", extensions=None):
    server = Flask(name, static_folder=static_folder, template_folder=template_folder)

    if not extensions:
        extensions = []

    success = server.config.from_envvar('APPLICATION_SETTINGS', silent=False)
    assert success

    for extension in extensions:
        extension.init_app(server)

    return server