import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.engine.symbol import Symbol, Group
from test.config import mongo


class TestEngine(object):
    def test_mock(self, mongo):
        with mongo as m:
            # Create a new page and add tags
            group = Group(name="US Equity")
            group.save()

            symbol = Symbol(name='IBM US Equity', group=group)
            # you can add on the dictionary on the fly
            symbol.tags = ['mongodb', 'mongoengine']

            symbol.reference["XXX"] = "A"
            symbol.reference["YYY"] = "B"

            symbol.save()

            assert Symbol.objects(tags='mongoengine').count() == 1
            assert symbol.group == group

    def test_collection(self, mongo):
        with mongo as m:
            # Create a new page and add tags
            group = Group(name="US Equity")
            group.save()

            symbol = Symbol(name='XXX', group=group)
            symbol.tags = ['mongodb', 'mongoengine']
            symbol.open = pd.Series(data=[1.1, 2.1, 3.1], name="test")
            symbol.save()

            #c = Correlation(name="Correlation", symbol1=symbol, symbol2=symbol)
            #c.data = PandasDocument.parse(pd.Series(data=[1, 2, 3], name="test"))
            #c.save()

            for s in Symbol.objects:
                pdt.assert_series_equal(s.open, pd.Series(data=[1.1, 2.1, 3.1], name="test"))
                try:
                    x = s.px_last
                except AttributeError:
                    x = None

                assert x is None
