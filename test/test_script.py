from pyutil.script import AbstractScript


class Script(AbstractScript):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self):
        pass


class TestScript(object):
    def test_script(self):
        s = Script(folder="folder")
        # logger is constructed implicitly
        assert s.logger
        #assert not s.mongo
        assert s["folder"] == "folder"
