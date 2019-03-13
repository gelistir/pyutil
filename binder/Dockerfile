FROM lobnek/jupyter:v2.3

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY ./work /home/${NB_USER}/work
COPY ./pyutil /home/${NB_USER}/work/pyutil

USER root
RUN conda install -y -c conda-forge xlrd xlsxwriter
USER ${NB_USER}
