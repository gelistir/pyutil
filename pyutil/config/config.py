# # -*- coding: utf-8 -*-
# """
#     flask.config
#     ~~~~~~~~~~~~
#
#     Implements the configuration related objects.
#
#     :copyright: 2010 Pallets
#     :license: BSD-3-Clause
# """
# import errno
# import os
# import types
#
#
# def configuration(envvar='APPLICATION_SETTINGS'):
#     # use the Config from Flask
#     config = Config(root_path=".")
#     success = config.from_envvar(envvar)
#
#     # make sure this was successful
#     assert success
#
#     return config
#
#
# class Config(dict):
#     """Works exactly like a dict but provides ways to fill it from files
#     or special dictionaries.  There are two common patterns to populate the
#     config.
#
#     Either you can fill the config from a config file::
#
#         app.config.from_pyfile('yourconfig.cfg')
#
#     Or alternatively you can define the configuration options in the
#     module that calls :meth:`from_object` or provide an import path to
#     a module that should be loaded.  It is also possible to tell it to
#     use the same module and with that provide the configuration values
#     just before the call::
#
#         DEBUG = True
#         SECRET_KEY = 'development key'
#         app.config.from_object(__name__)
#
#     In both cases (loading from any Python file or loading from modules),
#     only uppercase keys are added to the config.  This makes it possible to use
#     lowercase values in the config file for temporary values that are not added
#     to the config or to define the config keys in the same file that implements
#     the application.
#
#     Probably the most interesting way to load configurations is from an
#     environment variable pointing to a file::
#
#         app.config.from_envvar('YOURAPPLICATION_SETTINGS')
#
#     In this case before launching the application you have to set this
#     environment variable to the file you want to use.  On Linux and OS X
#     use the export statement::
#
#         export YOURAPPLICATION_SETTINGS='/path/to/config/file'
#
#     On windows use `set` instead.
#
#     :param root_path: path to which files are read relative from.  When the
#                       config object is created by the application, this is
#                       the application's :attr:`~flask.Flask.root_path`.
#     :param defaults: an optional dictionary of default values
#     """
#
#     def __init__(self, root_path, defaults=None):
#         dict.__init__(self, defaults or {})
#         self.root_path = root_path
#
#     def from_envvar(self, variable_name):
#         """Loads a configuration from an environment variable pointing to
#         a configuration file.  This is basically just a shortcut with nicer
#         error messages for this line of code::
#
#             app.config.from_pyfile(os.environ['YOURAPPLICATION_SETTINGS'])
#
#         :param variable_name: name of the environment variable
#         :return: bool. ``True`` if able to load config, ``False`` otherwise.
#         """
#         rv = os.environ.get(variable_name)
#         if not rv:
#             raise RuntimeError(
#                 "The environment variable %r is not set "
#                 "and as such configuration could not be "
#                 "loaded.  Set this variable and make it "
#                 "point to a configuration file" % variable_name
#             )
#         return self.from_pyfile(rv)
#
#     def from_pyfile(self, filename):
#         """Updates the values in the config from a Python file.  This function
#         behaves as if the file was imported as module with the
#         :meth:`from_object` function.
#
#         :param filename: the filename of the config.  This can either be an
#                          absolute filename or a filename relative to the
#                          root path.
#         """
#         filename = os.path.join(self.root_path, filename)
#         d = types.ModuleType("config")
#         d.__file__ = filename
#         try:
#             with open(filename, mode="rb") as config_file:
#                 exec(compile(config_file.read(), filename, "exec"), d.__dict__)
#                 for key in dir(d):
#                     if key.isupper():
#                         self[key] = getattr(d, key)
#
#         except IOError as e:
#             e.strerror = "Unable to load configuration file (%s)" % e.strerror
#             raise
#
#         return True
#
#     def __repr__(self):
#         return "<%s %s>" % (self.__class__.__name__, dict.__repr__(self))
#
#

