#!/bin/bash -x
source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# generate an urgent response package
echo "##########################################" 1>&2
echo -n "Scrapping BosSARCat for acquisitions: " 1>&2
date 1>&2
python $BASE_PATH/create_acquisitions.py > scrape_acqs.log 2>&1
STATUS=$?

echo -n "Finished scrapping acquisitions " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to scrape acquisitions." 1>&2
  echo "{}"
  exit $STATUS
fi