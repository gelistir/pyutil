from pyutil.script import AbstractScript


class Script(AbstractScript):
    def __init__(self, session, **kwargs):
        super().__init__(session, **kwargs)

    def run(self):
        pass


class TestScript(object):
    def test_script(self):
        s = Script(session=None, folder="folder")
        # logger is constructed implicitly
        assert s.logger

        assert not s.session
        assert not s.mongo

        assert s["folder"] == "folder"
