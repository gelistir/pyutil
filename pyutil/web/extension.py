from abc import abstractmethod
from flask import Flask, current_app


class InvalidSettingsError(Exception):
    pass


class Extension(object):
    def __init__(self, name, app=None, config=None):
        self.__name = name
        self.app = None
        if app is not None:
            self.init_app(app, config)

    @property
    def name(self):
        return self.__name

    def init_app(self, app, config=None):
        if not app or not isinstance(app, Flask):
            raise Exception('Invalid Flask application instance')

        self.app = app

        app.extensions = getattr(app, 'extensions', {})

        if self.name not in app.extensions:
            app.extensions[self.name] = {}

        if self in app.extensions[self.name]:
            # Raise an exception if extension already initialized as
            # potentially new configuration would not be loaded.
            raise Exception('Extension already initialized')

        if not config:
            # If not passed a config then we read the connection settings
            # from the app config.
            config = app.config

        # Store objects in application instance so that multiple apps do not
        # end up accessing the same objects.
        s = {'app': app, self.name: self.create(config)}
        app.extensions[self.name][self] = s

    @abstractmethod
    def create(self, config):
        raise NotImplementedError()

    @property
    def ext(self):
        return current_app.extensions[self.name][self][self.name]