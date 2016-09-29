from unittest import TestCase

from nose.tools import raises
from pyutil.decorators import attempt

@attempt
def f1(logger):
    assert False

@attempt
def f2(logger):
    return 5.0


@attempt
def f3():
    return 1.0


class TestDecorator(TestCase):
    @raises(AssertionError)
    def test_f1(self):
        # should throw an AssertError but shall inform a logger
        f1()

    def test_f2(self):
        self.assertEqual(f2(), 5.0)

    @raises(TypeError)
    def test_f3(self):
        # function needs to offer logger...
        f3()

