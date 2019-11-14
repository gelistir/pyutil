#!/usr/bin/env python

from setuptools import setup, find_packages
from pyutil import __version__ as version
setup(
    name='pyutil',
    version=version,
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.21.0', 'pandas>=0.24.0', 'sqlalchemy', 'psycopg2-binary', 'pymongo'],
    license='LICENSE.txt'
)
