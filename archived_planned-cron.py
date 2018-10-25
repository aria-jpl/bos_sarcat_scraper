import logging
from hysds_commons.job_utils import submit_mozart_job
import json

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

CRAWLER_QUEUE = "factotum-job_worker-small"


def submit_scrapper_job(params):
    rule = {
        "rule_name": "bos_sarcat_scrapper",
        "queue": CRAWLER_QUEUE,
        "priority": '8',
        "kwargs": '{}'
    }

    print('submitting jobs with param-s:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio={"id": "internal-temporary-wiring", "params": params,
                                                         "job-specification": "job-bos_ingest:master"},
                                      job_name='job_%s-%s' % ('bos_scrapper', "master"),
                                      enable_dedup=False)

    LOGGER.info("Job ID: " + mozart_job_id)
    print("Job ID: " + mozart_job_id)
    return


def construct_params():
    params = [
        {
            "name": "bos_ingest_time",
            "from": "value",
            "value": ""
        },
        {
            "name": "end_time",
            "from": "value",
            "value": ""
        },
        {
            "name": "from_time",
            "from": "value",
            "value": ""
        }
        ]
    print json.dumps(params)
    return params


def submit_scrapper():

    params = construct_params()
    submit_scrapper_job(params)
    return


if __name__ == '__main__':
    submit_scrapper()