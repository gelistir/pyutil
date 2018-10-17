from unittest import TestCase

import numpy as np
import pandas as pd
from mongoengine import connect

from pyutil.mongo.timeseries_old import Price


class TestMongo(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = connect('test', host="test-mongo")

        # get a fresh database
        cls.db.drop_database('test')

        x = Price(security="Thomas").save()
        x.series = pd.Series(data=np.random.randn(100000))

        # only now we store it!
        x.save()

    def test_read(self):
        print(dir(Price.objects))
        x = Price.objects(security="Thomas").first()
        x.series

        #print(x.series)




