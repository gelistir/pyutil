from pyutil.config.random import random_string


class TestRandom(object):
    def test_string(self):
        assert len(random_string(10)) == 10
        assert random_string(10) != random_string(10)
