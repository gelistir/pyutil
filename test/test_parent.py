from unittest import TestCase

from nose.tools import raises
from pyutil.parent import parent

class TestDecorator(TestCase):
    @raises(AssertionError)
    def test_f1(self):
        with parent() as p:
            raise AssertionError

