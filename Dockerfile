# Set the base image to Ubuntu
FROM continuumio/miniconda3

# lxml is causing a big mess only to make sure the premailer works. This has to be revisited and simplified...
COPY lib/libgcrypt.deb libgcrypt11_1.5.3-2ubuntu4.2_amd64.deb
RUN dpkg -i libgcrypt11_1.5.3-2ubuntu4.2_amd64.deb

RUN conda install -q -y pandas=0.19.1 requests=2.11.1 matplotlib pymongo xlrd=1.0.0 jinja2=2.8 xlsxwriter==0.9.3 lxml

# install a writer for excel...
RUN pip install raven==5.27.1 premailer

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil

# Import this to run construct the font-cache
RUN python -c "from matplotlib.font_manager import FontManager"
