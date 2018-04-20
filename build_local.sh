#!/bin/bash
rm -rf env
conda env create -p env -f production.yml
