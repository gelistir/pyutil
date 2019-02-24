# Set the base image to beakerx
FROM lobnek/jupyter:v1.0

COPY ./work /home/${NB_USER}/work
COPY ./pyutil /home/${NB_USER}/work/pyutil

RUN conda install -y xlrd xlsxwriter && conda clean -y --all