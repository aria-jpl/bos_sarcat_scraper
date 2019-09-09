import logging
import datetime
from hysds_commons.job_utils import submit_mozart_job
import json

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

CRAWLER_QUEUE = "factotum-job_worker-small"


def get_from_ingest_time():
    time = "{}Z".format((datetime.datetime.utcnow() - datetime.timedelta(days=2)).isoformat())
    return time


def submit_scrapper_job(params):
    rule = {
        "rule_name": "bos_sarcat_scrapper",
        "queue": CRAWLER_QUEUE,
        "priority": '8',
        "kwargs": '{}'
    }

    job_release = "master"
    job_name = "job-bos_ingest:%s" % job_release  # old job: "job-acquisition_ingest-bos:master"

    print('submitting jobs with param-s:')
    hysds_io = {
        "id": "internal-temporary-wiring",
        "params": params,
        "job-specification": job_name,
    }
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio=hysds_io, job_name=job_name, enable_dedup=False)

    LOGGER.info("Job ID: " + mozart_job_id)
    print("Job ID: " + mozart_job_id)
    return


def construct_params():
    params = [
        {
            "name": "bos_ingest_time",
            "from": "value",
            "value": get_from_ingest_time()
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
    print(json.dumps(params))
    return params


def submit_scrapper():

    params = construct_params()
    submit_scrapper_job(params)
    return


if __name__ == '__main__':
    submit_scrapper()
