import pandas.util.testing as pdt

from pyutil.mongo.engine.symbol import Symbol, Group
from test.config import *


@pytest.fixture()
def group():
    Group.objects.delete()
    return Group(name="US Equity").save()


@pytest.fixture()
def symbol(group):
    Symbol.objects.delete()

    symbol = Symbol(name='IBM US Equity', group=group, internal="IBM")
    # you can add on the dictionary on the fly
    symbol.tags = ['mongodb', 'mongoengine']

    symbol.reference["XXX"] = "A"
    symbol.reference["YYY"] = "B"
    symbol.open = pd.Series(data=[1.1, 2.1, 3.1], name="test")
    return symbol.save()


def test_symbol(group, symbol):
    assert Symbol.objects(tags='mongoengine').count() == 1
    assert symbol.group == group

    frame = Symbol.reference_frame()
    assert frame.loc["IBM US Equity"]["XXX"] == "A"
    assert Symbol.symbolmap() == {"IBM US Equity": "US Equity"}

    pdt.assert_series_equal(symbol.open, pd.Series(data=[1.1, 2.1, 3.1], name="test"))

    with pytest.raises(AttributeError):
        symbol.px_last

