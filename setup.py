#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pyutil',
    version='3.9.0',
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.21.0', 'pandas>=0.24.2', 'sqlalchemy', 'psycopg2-binary'],
    license='LICENSE.txt'
)
