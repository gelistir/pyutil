# Set the base image to Ubuntu, use a public image
# FROM continuumio/miniconda3:4.8.2 as builder
FROM python:3.7.7-slim-stretch as builder

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy requirements over
COPY requirements.txt /tmp/pyutil/requirements.txt

RUN buildDeps='gcc g++' && \
    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    rm -r /tmp/pyutil && \
    apt-get purge -y --auto-remove $buildDeps

COPY ./pyutil /pyutil/pyutil

#### Here the test-configuration
FROM builder as test

RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx mongomock requests-mock

WORKDIR /pyutil

CMD py.test --cov=pyutil  -vv --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html /pyutil/test