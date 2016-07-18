# Set the base image to Ubuntu
FROM continuumio/miniconda3

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

ADD . /pyutil
WORKDIR pyutil

RUN conda install -q -y --file production.txt && pip install -r requirements.txt


