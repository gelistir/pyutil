# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3 as builder

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy requirements over
COPY requirements.txt /tmp/pyutil/requirements.txt

# install pandas, requests, cvxpy and beakerx, install the requirements, clean traces
RUN conda install -y -c conda-forge nomkl pandas=0.25.3 requests=2.22.0 cvxpy=1.0.27 beakerx=1.4.1 && \
    conda clean -y --all && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    rm -r /tmp/pyutil

COPY ./pyutil /pyutil/pyutil

#### Here the test-configuration
FROM builder as test

RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx mongomock

WORKDIR /pyutil

CMD py.test --cov=pyutil  -vv --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html /pyutil/test