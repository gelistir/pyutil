# from io import BytesIO as _BytesIO
#
# import pandas as _pd
# import sqlalchemy as sq
#
# from pyutil.portfolio.portfolio import Portfolio as _Portfolio
# from pyutil.sql.base import Base
#
#
# #todo: remove soon
#
# class PortfolioSQL(Base):
#     __tablename__ = 'portfolio'
#     name = sq.Column(sq.String, primary_key=True)
#     _weights = sq.Column("weights", sq.LargeBinary)
#     _prices = sq.Column("prices", sq.LargeBinary)
#
#     @staticmethod
#     def _read(x):
#         json_str = _BytesIO(x).read().decode()
#         return _pd.read_json(json_str, orient="split")
#
#     @property
#     def portfolio(self):
#         return _Portfolio(weights=self.weight, prices=self.price)
#
#     @property
#     def weight(self):
#         try:
#             return self._read(self._weights)
#         except ValueError:
#             return _pd.DataFrame({})
#
#     @property
#     def price(self):
#         try:
#             return self._read(self._prices)
#         except ValueError:
#             return _pd.DataFrame({})
#
#     @property
#     def assets(self):
#         return self.portfolio.assets
