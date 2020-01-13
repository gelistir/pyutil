from pyutil.config.random import random_string


def test_string():
    assert len(random_string(10)) == 10
    assert random_string(10) != random_string(10)
