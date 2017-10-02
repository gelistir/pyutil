from unittest import TestCase

from pyutil.container.immutable import ImmutableDict


class TestContainer(TestCase):
    def test_dict(self):
        d = ImmutableDict({"a": 2, "b": 3})
        self.assertSetEqual(set(d.keys()), {"a","b"})
        self.assertSetEqual(set(d.values()), {2,3})
        self.assertEquals(len(d), 2)
        self.assertEquals(d["b"], 3)
        self.assertIsNotNone(d.__hash__())

        with self.assertRaises(TypeError):
            d["c"] = 10

        self.assertEquals(str(d), "{'a': 2, 'b': 3}")
        self.assertEquals(d, ImmutableDict({"a": 2, "b": 3}))




