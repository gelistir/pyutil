FROM lobnek/jupyter:v1.2

COPY ./work /home/${NB_USER}/work
COPY ./pyutil /home/${NB_USER}/work/pyutil

RUN conda install -y -c conda-forge xlrd xlsxwriter && conda clean -y --all