# Set the base image to beakerx
FROM jupyter/base-notebook:1386e2046833 as jupyter

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy the config.ini file
COPY /binder/jupyter_notebook_config.py /etc/jupyter/jupyter_notebook_config.py

# hardcoded parameters!? see https://github.com/moby/moby/issues/35018
COPY --chown=jovyan:users /binder/files $HOME/work

# copy the package over and install it
COPY --chown=jovyan:users . /tmp/pyutil

RUN pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    pip install --no-cache-dir /tmp/pyutil && \
    rm -rf /tmp/pyutil

RUN conda install -y -c conda-forge beakerx rise=5.5.0 cvxpy=1.0.15 && conda clean -y -all


