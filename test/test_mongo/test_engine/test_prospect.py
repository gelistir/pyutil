import pytest

from pyutil.mongo.engine.prospect import Prospect
from test.config import read


def test_prospect():
    c1 = Prospect(name="Peter Maffay",
                  position=read("prospect_position.csv", index_col=0, squeeze=True),
                  prices=read("price.csv", index_col=0, parse_dates=True))

    print(c1.position)
    print(c1.prices)

    with pytest.raises(AttributeError):
        print(c1.wurst)

    c1.portfolio(cash=0)

    with pytest.raises(AssertionError):
        Prospect(name="Peter")

    #assert c2.prices is None
