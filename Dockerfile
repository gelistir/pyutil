# Set the base image to Ubuntu
FROM continuumio/miniconda3

RUN conda install -y python=3.6 pandas=0.21 requests

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt && rm requirements.txt

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil
