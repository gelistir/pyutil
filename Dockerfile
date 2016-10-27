# Set the base image to Ubuntu
FROM continuumio/miniconda3

RUN conda install -q -y pandas=0.18.1 requests=2.9.1 matplotlib=1.5.1 pymongo=3.0.3 xlrd=1.0.0 jinja2=2.8

# install a writer for excel...
RUN pip install xlsxwriter==0.9.3 premailer==3.0.1 raven==5.27.1

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

ADD . /pyutil
WORKDIR pyutil

RUN mkdir -p results



