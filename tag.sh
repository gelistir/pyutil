#!/usr/bin/env bash
git tag -a $(python setup.py --version) -m "new tag"
git push --tags


