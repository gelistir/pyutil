# import os
#
# import pytest
#
# from pyutil.config.config import configuration, Config
#
#
# def test_configurations():
#     os.environ["wurst"] = "/pyutil/test/test_config/settings.cfg"
#     c = configuration(envvar="wurst")
#     assert c["MAILGUNAPI"] == 1
#
#
# def test_constructor():
#     c = Config(root_path=".")
#     os.environ["wurst"] = "/pyutil/test/test_config/settings.cfg"
#     c.from_envvar(variable_name="wurst")
#     assert c["MAILGUNAPI"] == 1
#     assert str(c) == "<Config {'MAILGUNAPI': 1, 'MAILGUNKEY': 2, 'READER': 4, 'TO_ADR': ['thomas@mailinator.com'], 'WRITER': 3}>"
#
#
# def test_not_exist():
#     c = Config(root_path=".")
#     os.environ["wurst"] = "/pyutil/test/test_config/nonono.cfg"
#     with pytest.raises(FileNotFoundError):
#         c.from_envvar(variable_name="wurst")
#
#
# def test_envvar_not_exist():
#     c = Config(root_path=".")
#     with pytest.raises(RuntimeError):
#         c.from_envvar(variable_name="wurst2")
