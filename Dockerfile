# Set the base image to Ubuntu, use a public image
FROM continuumio/miniconda3

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    conda update -y conda && \
    conda install -y nomkl pandas=0.21.0 requests xlrd xlsxwriter && \
    pip install --no-cache-dir  -r requirements.txt && rm requirements.txt && \
    conda clean -y --all

# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil
