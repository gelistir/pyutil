#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pyutil',
    version='3.3.9',
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.13.0', 'pandas>=0.21.0', 'sqlalchemy']
)
