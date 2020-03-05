FROM jupyter/base-notebook:1386e2046833 as jupyter

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy the config file
COPY ./binder/jupyter_notebook_config.py /etc/jupyter/jupyter_notebook_config.py

# copy the package over and install it
COPY --chown=jovyan:users . /tmp/pyutil

RUN conda install -y -c conda-forge nomkl pandas=0.25.3 requests=2.22.0 cvxpy mongomock beakerx && \
    conda clean -y --all && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    pip install --no-cache-dir /tmp/pyutil && \
    rm -rf /tmp/pyutil


# ----------------------------------------------------------------------------------------
FROM jupyter as jupyterlab

RUN conda install -y -c conda-forge jupyterlab && \
    jupyter labextension install @jupyter-widgets/jupyterlab-manager && \
    jupyter labextension install beakerx-jupyterlab && \
    #jupyter labextension install @jupyterlab/celltags && \
    jupyter lab build

# hardcoded parameters!? see https://github.com/moby/moby/issues/35018
COPY --chown=jovyan:users /binder/files $HOME/work