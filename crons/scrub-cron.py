import logging
from hysds_commons.job_utils import submit_mozart_job
import json

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

CRAWLER_QUEUE = "factotum-job_worker-small"


def submit_scrubber_job(params):

    rule = {
        "rule_name": "bos_sarcat_scrubber",
        "queue": CRAWLER_QUEUE,
        "priority": '8',
        "kwargs": '{}'
    }

    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio={"id": "internal-temporary-wiring", "params": params,
                                                         "job-specification": "job-scrub_outdated_bos_acqs:master"},
                                      job_name='job_%s-%s' % ('scrub_outdated_bos_acqs', "master"),
                                      enable_dedup=False)

    LOGGER.info("Job ID: " + mozart_job_id)
    print("Job ID: " + mozart_job_id)
    return

def submit_scrapper():
    params = []
    submit_scrubber_job(params)
    return


if __name__ == '__main__':
    submit_scrapper()