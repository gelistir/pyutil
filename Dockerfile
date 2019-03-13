# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3 as builder

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    conda update -y conda && \
    conda install -y -c conda-forge nomkl pandas=0.24.1 requests=2.21.0 xlrd xlsxwriter cvxpy=1.0.14 && \
    pip install --no-cache-dir  -r requirements.txt && rm requirements.txt && \
    conda clean -y --all


# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil

#### Here the test-configuration

FROM builder as test

# We install flask here to test some
RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx flask==1.0.2

COPY ./test            /pyutil/test
COPY ./sphinx.sh       /pyutil/sphinx.sh

CMD py.test --cov=pyutil  --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html test