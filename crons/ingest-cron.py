import argparse
import logging
import datetime
from hysds_commons.job_utils import submit_mozart_job
import json

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

CRAWLER_QUEUE = "factotum-job_worker-large"


def get_from_ingest_time(days_delta, hours_delta):
    """
    calculated bos ingest time based on delta in days or hours
    :param days_delta:
    :param hours_delta:
    :return:
    """
    if days_delta is not None:
        time = "{}Z".format((datetime.datetime.utcnow() - datetime.timedelta(days=days_delta)).isoformat())
    elif hours_delta is not None:
        time = "{}Z".format((datetime.datetime.utcnow() - datetime.timedelta(hours=hours_delta)).isoformat())
    return time


def submit_scraper_job(job_type, tag, params):
    """
    Submits job
    :param job_type:
    :param tag:
    :param params:
    :return:
    """
    rule = {
        "rule_name": "bos_sarcat_scraper",
        "queue": CRAWLER_QUEUE,
        "priority": '8',
        "kwargs": '{}'
    }

    print('submitting jobs with param-s:')
    job_spec = '%s:%s' % (job_type, tag)
    job_name = '%s-%s' % (job_type, tag)
    hysds_ios = {
        "id": "internal-temporary-wiring",
        "params": params,
        "job-specification": job_spec
    }
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio=hysds_ios, job_name=job_name, enable_dedup=False)

    LOGGER.info("Job ID: " + mozart_job_id)
    print("Job ID: " + mozart_job_id)
    return


def construct_params(days_delta, hours_delta):
    """
    Constructs job parameter based on specified time delta
    :param days_delta:
    :param hours_delta:
    :return:
    """
    params = [
        {
            "name": "bos_ingest_time",
            "from": "value",
            "value": get_from_ingest_time(days_delta, hours_delta)
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


def submit_scrapper(job_type, tag, days_delta, hours_delta):
    """
    Construct parameters based on specified deltas and submit job
    :param job_type:
    :param tag:
    :param days_delta:
    :param hours_delta:
    :return:
    """

    params = construct_params(days_delta, hours_delta)
    submit_scraper_job(job_type, tag, params)
    return


if __name__ == '__main__':
    '''
    Main program that is run by cron to submit a scraper job
    '''

    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", help="Delta in days", nargs='?',
                       type=int, required=False)
    group.add_argument("--hours", help="Delta in hours", nargs='?',
                       type=int, required=False)
    parser.add_argument("--tag", help="PGE docker image tag (release, version, " +
                                      "or branch) to propagate",
                        default="master", required=True)

    args = parser.parse_args()
    delta_days = args.days
    delta_hours = args.hours
    release_tag = args.tag
    job = "job-bos_ingest"
    submit_scrapper(job, release_tag, delta_days, delta_hours)
