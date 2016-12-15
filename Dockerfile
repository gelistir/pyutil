# Set the base image to Ubuntu
FROM continuumio/miniconda3

RUN conda install -q -y pandas=0.19.1 requests=2.11.1 matplotlib pymongo xlrd=1.0.0 jinja2=2.8 xlsxwriter==0.9.3 lxml==3.6.4

# install a writer for excel...
RUN pip install premailer==3.0.1 raven==5.27.1

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy only the package
COPY ./pyutil /pyutil

WORKDIR pyutil

# Import this to run construct the font-cache
# RUN python -c "from matplotlib.font_manager import FontManager"
