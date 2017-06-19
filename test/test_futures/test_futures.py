from unittest import TestCase

import pandas as pd

from pyutil.futures.futures import Futures, Contracts

# An empty database requires special care, we do that here...
from test.config import connect


class TestFutures(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()

    def test_init(self):
        properties={"FUT_GEN_MONTH": "HMUZ"}

        f = Futures(name="A", internal="A_internal", properties=properties)
        self.assertEquals(f.name,"A")
        self.assertEquals(f.internal,"A_internal")
        self.assertListEqual(f.gen_month, ["H","M","U","Z"])
        self.assertEquals(f.ref["FUT_GEN_MONTH"], "HMUZ")
        self.assertTrue("H" in f.gen_month)
        self.assertFalse("A" in f.gen_month)


    def test_contracts_emtpy(self):
        f = Futures(name="A", internal="A_internal").save()
        self.assertListEqual(f.contracts(), [])

    def test_contracts(self):
        f = Futures(name="A", internal="A_internal", properties={"FUT_GEN_MONTH": "HMUZ"}).save()

        c1 = Contracts(future=f, name="A_1", figi="1", notice=pd.Timestamp("2010-01-01"), properties={"FUT_MONTH_YR": "MAR 07"}).save()
        c2 = Contracts(future=f, name="A_2", figi="2", notice=pd.Timestamp("2011-01-01"), properties={"FUT_MONTH_YR": "JUN 07"}).save()
        c3 = Contracts(future=f, name="A_3", figi="3", notice=pd.Timestamp("2012-01-01"), properties={"FUT_MONTH_YR": "MAR 08"}).save()
        c4 = Contracts(future=f, name="A_4", figi="4", notice=pd.Timestamp("2019-01-01"), properties={"FUT_MONTH_YR": "APR 08"}).save()

        self.assertListEqual(f.figis(only_liquid=True), ["1","2","3"])
        self.assertListEqual(f.figis(only_liquid=False), ["1","2","3","4"])

        x = f.update_figis(figis=["4","5"])
        self.assertListEqual(x, ["5"])

        g = f.roll_map(only_liquid=True, offset_days=5)

        self.assertListEqual(g.contracts, [c1, c2, c3])
        self.assertEquals(g[pd.Timestamp("2010-12-27")], c3)

        alive = [c for c in Contracts.alive(today=pd.Timestamp("2017-01-01"))]
        self.assertListEqual(alive, [c4])




