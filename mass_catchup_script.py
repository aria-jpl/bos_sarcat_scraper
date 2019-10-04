from builtins import range
import sys
import datetime, time
from hysds_commons.job_utils import submit_mozart_job


def generate_date_range(start_date, end_date, delta_hours):
    # This script is meant to submit acq scrape jobs for a time period
    d = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    date_list = list()
    delta = datetime.timedelta(hours=int(delta_hours))
    while d <= end_date:
        date_list.append(datetime.datetime.strftime(d, '%Y-%m-%dT%H:%M:%S.%fZ'))
        d += delta
    return date_list


def submit_job(start, end, job_queue):
    tag = 'master'
    job_spec = 'job-bos_ingest:%s' % tag

    start_time_tag = start.replace('-', '').replace(':', '')
    end_time_tag = end.replace('-', '').replace(':', '')
    job_name = '%s-%s-%s' % (job_spec, start_time_tag, end_time_tag)

    # Setup input arguments here
    rule = {
        'rule_name': 'bos_sarcat_scraper',
        'queue': job_queue,
        'priority': '7',
        'kwargs': '{}'
    }

    params = [
        {
          'name': 'bos_ingest_time',
          'from': 'value',
          'value': ''
        },
        {
          'name': 'from_time',
          'from': 'value',
          'value': start
        },
        {
          'name': 'end_time',
          'from': 'value',
          'value': end
        }
    ]

    hysds_io = {
      'id': 'internal-temporary-wiring',
      'params': params,
      'job-specification': job_spec
    }
    job_id = submit_mozart_job({}, rule, hysdsio=hysds_io, job_name=job_name)

    print('Submitted job for window {} to {}, JOB ID: {}'.format(start_time, end_time, job_id))
    print('Submitted job for window {} to {}'.format(start_time, end_time, id))


if __name__ == '__main__':
    'Main program that is run by to submit a scraper jobs'
    start_time = sys.argv[1]  # ex. 2019-08-01
    end_time = sys.argv[2]  # ex. 2019-09-01
    hours_delta = sys.argv[3]  # ex. 2
    queue = sys.argv[4]  # ex. factotum-job_worker-large
    # example command: python mass_catchup_script.py 2019-08-01 2019-09-01 2 factotum-job_worker-large

    dates = generate_date_range(start_date=start_time, end_date=end_time, delta_hours=hours_delta)

    for i in range(len(dates) - 1):
        submit_job(dates[i], dates[i + 1], queue)
        print(dates[i], dates[i + 1])
        time.sleep(60)
