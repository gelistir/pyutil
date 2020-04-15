# pyutil

[![Build Status](https://travis-ci.org/lobnek/pyutil.svg?branch=master)](https://travis-ci.org/lobnek/pyutil)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/lobnek/pyutil/master)

Set of utility code used by Lobnek Wealth Management.


## Installation
```python
pip install lob-pyutil
```

## Utility code

Lobnek Wealth Management is a Swiss Family office. We run quantitative and quantamental strategies for our clients based on Python scripts.
Neither the strategies nor the clients are published here but we have decided to share some of the tools we use to create and 
maintain our systems. 

We have released tools for 
* Convex programming (with cvxpy)
* Database management (with MongoDB and MongoEngine)
* Configurations and logging 
* Performance measurement (drawdown, year/month tables, etc.)
* Plots with beakerx
* Portfolios (construction and analysis, tools to reduce trading activity)
* Strategy development (all following the same blueprint)
* (Unit)test support
* Time series data (oscillators, etc.)
* Web development

We are most interested in your feedback and appreciate comments and support contributions for our tools.  We recommend to explore
our tools following the binder link given above.

In this project we also demonstrate the use of docker and take advantage of Binder, see https://mybinder.org/v2/gh/lobnek/pyutil/master
We combine Docker with an old-school Makefile approach and achieve a test-coverage of 100%.

All our (internal) web services and strategies are based on this package. 

