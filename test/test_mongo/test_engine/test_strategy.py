from pyutil.mongo.engine.strategy import Strategy
from test.config import mongo_client


class TestEngine(object):

    def test_strategy(self, mongo_client):
        s = Strategy(name="mdt", type="mdt", active=True, source="AAA")

        assert s.source == "AAA"
        assert s.type == "mdt"
        assert s.active

        assert s.portfolio is None
        assert s.last_valid_index is None
        s.save()

        frame = Strategy.reference_frame()
        assert frame.index.name == "strategy"
