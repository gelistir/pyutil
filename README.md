# pyutil

Set of utility code used by Lobnek Wealth Management.
[![Build Status](https://travis-ci.org/lobnek/pyutil.svg?branch=master)](https://travis-ci.org/lobnek/pyutil)

## Installation
```python
pip install git+http://github.com/lobnek/pyutil.git
```

## Running a strategy

![Alt text](examples/portfolio.png)

Our main concern is to implement and maintain strategies in a robust way. We do not rewrite our codes for production servers 
in alternative programming languages. We use the same Python scripts both in research and production. 

A strategy is a script loading time series data from an archive and using parameters specified a priori.
For research it is helpful to interfere with the parameters before the strategy iterates in a backtest through history.

It is possible to point the strategy to different reader (objects). 
For testing purposes we fire up and populate a MongoDB Docker image 
whereas in production we run a MongoDB server providing access to our latest data.

Each strategy is described by a class Configuration and is a child of the ConfigMaster class.
Although inheritance is rarely used in Python we use it here to enforce a small common interface for all strategies.

```python
	from pyutil.strategy.ConfigMaster import ConfigMaster
	
	
	class Configuration(ConfigMaster):
		def __init__(self, reader, names, logger=None):
			super().__init__(reader=reader, names=names, logger=logger)
			
```

Once we have instantiated a Configuration object we could still modify the configuration dictionary which is a simple
member variable. In this example we define the parameters start.

```python
     c = Configuration(reader, names=["A","B","C"])
     c.configurations["risk"]=1.0

```

We shall talk briefly about Readers. Readers are functions 
providing access to historice data and reference data for any asset

```python
    def reader(name):
        return Asset(...)
        
```
The actual implementation depends of course on the underlying technology used to store this data.

So far, we have only mentioned to instantiate a Configuration object and pointed it to an archive. To build the portfolio the
ConfigMaster is exposing the abstract portfolio method, e.g. we extend the Configuration class by 

```python
	from pyutil.portfolio.portfolio import Portfolio
	from pyutil.strategy.ConfigMaster import ConfigMaster
	
	
	class Configuration(ConfigMaster):
		def __init__(self, reader, logger=None):
			super().__init__(reader=reader, names=["A", "B", "C"], logger=logger)
	
		def portfolio(self):
			# extract the assets (using the reader)
			p = self.assets.history["PX_LAST"]
			return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / 3.0))
```
The actual strategy is therefore executed as 

```python	
	# define a Configuration object
	c = Configuration(reader=reader, names=["A","B","C"])
	
	# modify the configuration dictionary here...
	c.configuration["Peter"]="Maffay"
	
	# compute the portfolio
	p = c.portfolio()
```

There is a wealth of tools to analyse the portfolio objects. 