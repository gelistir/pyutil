from unittest import TestCase

from pyutil.parent import Parent


class TestDecorator(TestCase):

    def test_f2(self):
        with Parent() as parent:
            parent.logger.info("Hello")
            a = 4
        self.assertEqual(a, 4)

    def test_context(self):
        with Parent() as f:
            print(f)
            print(type(f))
            for i in range(1000000):
                a = i * i
            print(f.elapsed)
            # print(1/0)
            print(4)
