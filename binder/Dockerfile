FROM lobnek/jupyter:v1.1

COPY ./work /home/${NB_USER}/work
COPY ./pyutil /home/${NB_USER}/work/pyutil

RUN conda install -y -c conda-forge xlrd xlsxwriter cvxpy=1.0.14 && conda clean -y --all