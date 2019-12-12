FROM jupyter/base-notebook:1386e2046833 as jupyter

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy the config.ini file
COPY /binder/jupyter_notebook_config.py /etc/jupyter/jupyter_notebook_config.py

# hardcoded parameters!? see https://github.com/moby/moby/issues/35018
COPY --chown=jovyan:users /binder/files $HOME/work

# copy the package over and install it
COPY --chown=jovyan:users . /tmp/pyutil

RUN conda install -y -c conda-forge nomkl pandas=0.24.2 requests=2.21.0 cvxpy mongomock beakerx && \
    conda clean -y --all && \
    pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    pip install --no-cache-dir /tmp/pyutil && \
    rm -rf /tmp/pyutil

RUN conda install -y -c conda-forge pyarrow=0.15.1 && \
    conda clean -y --all

#RUN conda install -y -c conda-forge cvxpy && conda clean -y -all

    #conda install -c plotly -c defaults -c conda-forge "jupyterlab>=1.0" jupyterlab-dash && \
    #pip install dash_daq==0.2.2 && \
#RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager && \
#    #jupyter labextension install jupyterlab-dash && \
#    jupyter labextension install beakerx-jupyterlab && \
#    jupyter labextension install @jupyterlab/celltags && \
#    jupyter lab build
