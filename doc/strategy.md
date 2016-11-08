The strategy package provides the framework for running automated trading strategy.
Each strategy is an implementation of the abstract ConfigMaster class.

This class is exposing a dictionary for the underlying configuration (e.g. parameters, assets, etc.). 
In backtests it is possible to alter those parameters before (re)running the strategy.

Here's such a strategy

    import pandas as pd
    
    from pyutil.portfolio.portfolio import Portfolio
    from pyutil.strategy.ConfigMaster import ConfigMaster
    
    
    class Configuration(ConfigMaster):
        def __init__(self, archive, t0, logger=None):
            super().__init__(archive=archive, t0=t0, logger=logger)
            self.configuration["prices"] = self.prices(["A", "B", "C"])
    
        def portfolio(self):
            p = self.configuration["prices"]
            return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / len(p.keys())))
    
    
        @property
        def name(self):
            return "test"
    
        @property
        def group(self):
            return "testgroup"
            
The strategy is fed by an archive variable. This variable is the wrapper we use to access our data.
It is then extracting the prices for the assets A,B and C. This data is exposed in the public configuration dictionary.
Therefore it is possible to alter those prices, e.g. if data has to be replaced, deleted, etc.

Only when calling the portfolio method the actual portfolio will be computed using the state of the configuration dictionary.

