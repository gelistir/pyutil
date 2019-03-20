FROM lobnek/jupyter:v2.5

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY . /tmp/pyutil
COPY ./work ${WORK}



#COPY ./pyutil ${WORK}/pyutil

USER root
RUN conda install -y -c conda-forge xlrd xlsxwriter && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    pip install --no-cache-dir /tmp/pyutil && \
    rm -r /tmp/pyutil

USER ${NB_USER}
