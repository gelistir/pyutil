#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pyutil',
    version='3.8.3',
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.19.0', 'pandas>=0.23.1', 'sqlalchemy', 'psycopg2-binary']
)
