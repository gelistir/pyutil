#!./env/bin/python

from setuptools import setup, find_packages

setup(
    name='pyutil',
    version='v0.2.5',
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    description='', install_requires=['requests>=2.8.1', 'pandas>=0.17.0']
)
