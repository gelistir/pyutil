# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3 as builder

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    conda update -y conda && \
    apt-get -y update && \
    apt-get install -y postgresql-client && \
    conda install -y nomkl pandas=0.23.4 requests xlrd xlsxwriter && \
    conda install -c conda-forge && \
    pip install --no-cache-dir  -r requirements.txt && rm requirements.txt && \
    conda clean -y --all


# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil

#### Here the test-configuration

FROM builder as test

RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx

COPY ./test            /pyutil/test
COPY ./sphinx.sh       /pyutil/sphinx.sh

CMD py.test --cov=pyutil  --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html test