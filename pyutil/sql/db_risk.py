# from pyutil.sql.db import Database
# from pyutil.sql.interfaces.risk.owner import Owner
# from pyutil.sql.interfaces.risk.security import Security
#
#
# class DatabaseRisk(Database):
#     def __init__(self, client, session=None):
#         super().__init__(client=client, session=session, db="addepar2")
#
#     def owner(self, name):
#         return self._filter(Owner, name=str(name))
#
#     def security(self, name):
#         return self._filter(Security, name=str(name))
#
#     # rename the columns apply int!!!!
#     @property
#     def prices(self):
#         return Security.prices_all(client=self.client)
#
#     # same
#     @property
#     def returns(self):
#         return Owner.returns_all(client=self.client)
#
#     @property
#     def reference_owner(self):
#         return Owner.reference_frame(self.session.query(Owner))
#
#     @property
#     def reference_securities(self):
#         return Security.reference_frame(self.session.query(Security))
#
#     @property
#     def reference_owner_securities(self):
#         return Owner.reference_frame_securities(self.session.query(Owner))
#
#     # apply int
#     @property
#     def position(self):
#         # read all positions at once. This is fast!
#         return Owner.position_all(client=self.client).swaplevel(i=0,j=-1).swaplevel(i=0,j=1)
#
#     # apply int
#     @property
#     def volatility_owner(self):
#         return Owner.volatility_all(client=self.client)
#
#
#     # apply int
#     @property
#     def volatility_security(self):
#         return Security.volatility_all(client=self.client).unstack(level="security").swaplevel()
#
#     #@property
#     #def volatility_owner_securities(self):
#     #    return self.__client.frame(measurement="")
#     #    vola = self._read(sql="SELECT * FROM v_volatility_owner_security", index_col=["owner", "security"])["data"]
#     #    return vola.apply(to_pandas)



