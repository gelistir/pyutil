# Set the base image to Ubuntu
FROM continuumio/miniconda3

RUN conda install -q -y python=3.6 pandas requests matplotlib pymongo openpyxl

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt && rm requirements.txt


# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

# copy only the package
COPY ./pyutil /pyutil/pyutil

WORKDIR pyutil

# Import this to run construct the font-cache
RUN python -c "from matplotlib.font_manager import FontManager"
