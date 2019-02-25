FROM lobnek/jupyter:v1.1

COPY ./work /home/${NB_USER}/work
COPY ./pyutil /home/${NB_USER}/work/pyutil

RUN conda install -y xlrd xlsxwriter && conda clean -y --all