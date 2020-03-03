#!/usr/bin/env python

from setuptools import setup, find_packages
from pyutil import __version__ as version

# read the contents of your README file
with open('README.md') as f:
    long_description = f.read()

setup(
    name='lob-pyutil',
    long_description=long_description,
    long_description_content_type='text/markdown',
    version=version,
    packages=find_packages(include=["pyutil*"]),
    author='Lobnek Wealth Management',
    author_email='thomas.schmelzer@lobnek.com',
    url='https://github.com/lobnek/pyutil',
    description='Utility code of a Swiss Family Office',
    install_requires=['requests>=2.22.0', 'pandas>=0.25.3', 'pymongo', 'scipy', 'mongoengine'],
    license="MIT"
)
