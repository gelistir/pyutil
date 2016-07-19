# pyutil

![Alt text](portfolio.png)

The attempts to test using an in-memory version of MongoDB failed.

# Running a strategy

Our main concern is to implement and maintain strategies in a robust way. We do not rewrite our codes for production servers 
in alternative programming languages. We use the same Python scripts both in research and production. 

A strategy is a script loading time series data from an archive and using parameters specified a priori.
For research it is helpful to interfere and alternate the parameters before the strategy iterates in a backtest through history.

It is possible to point the strategy to different archives. For testing purposes we use an archive with immutable test data 
whereas in production we run a MongoDB server providing access to our latest data.

Each strategy is described by a class Configuration and is a child of the ConfigMaster class.
Inheritance is rarely used in Python. Here we use it to enforce a small common interface for all strategies.

```python
	from pyutil.strategy.ConfigMaster import ConfigMaster
	
	
	class Configuration(ConfigMaster):
		def __init__(self, archive, logger=None):
			super().__init__(archive=archive, logger=logger)
			self.configuration["assets"] = ["A", "B", "C"]
			self.configuration["start"] = pd.Timestamp("2002-01-01")
```

Once we have instantiated a Configuration object we could still modify the configuration dictionary which is a simple
member variable. In this example we define the parameters assets and start.

```python
     c = Configuration(archive)
     c.configuration["assets"] = ["A", "B", "D"]
```

We replace the asset C by D. We shall talk briefly about Archives. Archives provide 
read access to historic data. 

```python
	class Archive(object):
		__metaclass__ = abc.ABCMeta
	
		@abc.abstractmethod
		def history(self, items, name, before):
			return
```

So far, we have only mentioned to instantiate a Configuration object and pointed it to an archive. To build the portfolio the
ConfigMaster is exposing the abstract portfolio method, e.g. we extend the Configuration class by 

```python
	import pandas as pd
	
	from pyutil.portfolio.portfolio import Portfolio
	from pyutil.strategy.ConfigMaster import ConfigMaster
	
	
	class Configuration(ConfigMaster):
		def __init__(self, archive, logger=None):
			super().__init__(archive=archive, logger=logger)
			self.configuration["assets"] = ["A", "B", "C"]
			self.configuration["start"] = pd.Timestamp("2002-01-01")
			
		def portfolio(self):
			a = self.configuration["assets"]
			p = self.archive.history(items=a, before=self.configuration["start"])
			return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0/len(a)))
```
The actual strategy is therefore executed as 

```python
	# define an archive
	archive = ArchiveReader()
	
	# define a Configuration object
	c = Configuration(archive=archive)
	
	# alternate any parameters for your backtest
	c.configurations["assets"]=["A","B","D"]
	
	# compute the portfolio
	p = c.portfolio()
```

There is a wealth of tools to analyse the portfolio objects. 