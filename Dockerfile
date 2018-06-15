# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3 as builder

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    conda update -y conda && \
    conda install -y nomkl pandas=0.21.0 requests xlrd xlsxwriter && \
    pip install --no-cache-dir  -r requirements.txt && rm requirements.txt && \
    conda clean -y --all

# install the postgresql client...
RUN apt-get update && apt-get install -y postgresql-client

# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil

#### Here the test-configuration

FROM builder as test

RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx

COPY ./test            /pyutil/test
#COPY ./wait-for-postgres.sh /pyutil/wait-for-postgres.sh

CMD py.test --cov=pyutil  --cov-report html:/html-coverage --cov-report term --html=/html-report/report.html test