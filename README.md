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


