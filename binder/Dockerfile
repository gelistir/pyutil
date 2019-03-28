FROM lobnek/jupyter:v2.8

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@lobnek.com"

COPY --chown=beakerx:beakerx . /tmp/pyutil
COPY --chown=beakerx:beakerx ./work ${WORK}

RUN pip install --no-cache-dir -r /tmp/pyutil/requirements.txt && \
    pip install --no-cache-dir /tmp/pyutil && \
    rm -r /tmp/pyutil
