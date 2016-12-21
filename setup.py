#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pyutil',
    version='v1.9.7',
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.11.1', 'pandas>=0.19.0']
)
