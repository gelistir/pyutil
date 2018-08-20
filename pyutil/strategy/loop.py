# import logging
# import os
#
# from pyutil.sql.interfaces.symbols.strategy import Strategy, module
# from pyutil.sql.interfaces.symbols.symbol import Symbol
# from pyutil.sql.session import get_one_or_create
#
#
#
# def strategy_loop(session, folder="/lobnek/strat", reader=None, logger=None):
#     logger = logger or logging.getLogger(__name__)
#
#     reader = reader or Symbol.symbol
#
#     for sfile in os.listdir(folder):
#         logger.debug("File {f}".format(f=sfile))
#         with open(os.path.join(folder, sfile), "r") as f:
#             source = f.read()
#
#             m = module(source=source)
#
#             configuration = m.Configuration(reader=reader)
#
#             strat, exists = get_one_or_create(session=session, model=Strategy, name=configuration.name)
#
#             logger.debug("Strategy: {s}".format(s=strat.name))
#             logger.debug("Last stamp {s}".format(s=strat.last))
#
#             strat.active = True
#             strat.source = source
#
#             yield configuration.portfolio, strat
