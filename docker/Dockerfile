FROM hysds/pge-base:python2

MAINTAINER malarout "namrata.malarout@jpl.nasa.gov"
LABEL description="Bos SARCat Acquisitions Scrapper"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/bos_sarcat_scrapper
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/bos_sarcat_scrapper

RUN cd /home/ops/verdi/ops/bos_sarcat_scrapper \
 && sudo pip install -e .

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
