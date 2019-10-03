FROM hysds/pge-base:latest

MAINTAINER malarout "namrata.malarout@jpl.nasa.gov"
LABEL description="Bos SARCat Acquisitions Scraper"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/bos_sarcat_scraper
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/bos_sarcat_scraper \
 && cd /home/ops/verdi/ops/bos_sarcat_scraper \
 && pip install -e . \
 && pip install elasticsearch

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
