# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3 as builder

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY requirements.txt /tmp/pyutil/requirements.txt

RUN conda install -y -c conda-forge nomkl pandas=1.0.1 requests=2.22.0 scipy beakerx=1.4.1 && \
    conda clean -y --all && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    rm -r /tmp/pyutil

COPY ./pyutil /pyutil/pyutil

#### Here the test-configuration
FROM builder as test

RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx flask==1.0.2 mongomock

WORKDIR /pyutil

CMD py.test --cov=pyutil  -v --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html /pyutil/test