FROM lobnek/jupyter:v2.5

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY ./work ${WORK}
COPY ./pyutil ${WORK}/pyutil

USER root
RUN conda install -y -c conda-forge xlrd xlsxwriter
USER ${NB_USER}
