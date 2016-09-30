import logging
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

@attempt
def f4(logger, x, y):
    return x+y

class TestDecorator(TestCase):
    @raises(AssertionError)
    def test_f1(self):
        # should throw an AssertError and shall inform a logger
        f1()

    def test_f2(self):
        self.assertEqual(f2(), 5.0)

    @raises(TypeError)
    def test_f3(self):
        # function needs to offer logger...
        f3()


    def test_f4(self):
        self.assertEqual(f4(x=2, y=3), 5)

    def test_attempt(self):
        def f(logger):
            return 2.0

        g = attempt(f)
        self.assertEqual(g(), 2.0)

    def test_logger(self):
        logger = logging.getLogger(__name__)
        def f(logger):
            return 2.0

        g = attempt(f)
        self.assertEqual(g(logger), 2.0)
