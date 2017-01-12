import os
from unittest import TestCase

from pyutil.strategy.Loop import loop


class TestLoop(TestCase):
    def test_loop(self):
        path = "/pyutil/test/test_strategy/scripts"

        length = len([x for x in loop(path=path, prefix="test.test_strategy.scripts.")])
        self.assertEquals(length, 2)

    def test_assert(self):
        path = "/pyutil/test/test_strategy/scripts"
        with self.assertRaises(AssertionError):
            len([x for x in loop(path=2, prefix="test.test_strategy.scripts.")])

    def test_dont_exist(self):
        path = "/abc"
        with self.assertRaises(AssertionError):
            len([x for x in loop(path=path, prefix="test.test_strategy.scripts.")])


