from unittest import TestCase

from nose.tools import raises
from pyutil.parent import Parent


class TestDecorator(TestCase):
    @raises(AssertionError)
    def test_f1(self):
        with Parent() as parent:
            print(type(parent))
            raise AssertionError

    def test_f2(self):
        with Parent() as parent:
            parent.logger.info("Hello")
            a = 4
        self.assertEquals(a,4)

    def test_context(self):
        with Parent() as f:
            print(f)
            print(type(f))
            for i in range(1000000):
                a = i*i
            print(f.elapsed)
            #print(1/0)
            print(4)

