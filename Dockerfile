# Set the base image to Ubuntu
FROM lobnek/pybase:v1.3

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

ADD . /pyutil
WORKDIR pyutil

RUN mkdir -p results



