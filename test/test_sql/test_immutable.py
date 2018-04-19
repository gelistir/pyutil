from unittest import TestCase

from pyutil.sql.immutable import ReadList
from pyutil.sql.immutable import ReadDict

class TestImmutable(TestCase):
    def test_list(self):
        x = ReadList([1,2,3], cls=int)
        with self.assertRaises(TypeError):
            x.append(4)

        with self.assertRaises(TypeError):
            x.extend([4,5])

        with self.assertRaises(TypeError):
            x.pop(3)

        with self.assertRaises(TypeError):
            x[0] = 5

    def test_dict(self):
        x = ReadDict({"a": 2.0, "b": 3.0})

        with self.assertRaises(TypeError):
            x["a"] = 4.0

        with self.assertRaises(TypeError):
            x.pop("a")




