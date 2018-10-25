#!/bin/bash -x
source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# remove planned, predicted acquisitions that are now-2days or older
echo "##########################################" 1>&2
echo -n "Srubbing outdated BosSARCat acquisitions: " 1>&2
date 1>&2
python $BASE_PATH/purge_past_planned_predicted.py > scrub_acqs.log 2>&1
STATUS=$?

echo -n "Finished scrubbing outdated planned and predicted acquisitions " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to scrub outdated acquisitions." 1>&2
  echo "{}"
  exit $STATUS
fi