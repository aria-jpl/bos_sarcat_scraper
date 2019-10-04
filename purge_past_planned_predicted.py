#!/usr/bin/env python

'''
This script is for purging planned and predicted
acquisitions in sarcat index that occured 2 days ago
'''

import os
import copy
from datetime import datetime, timedelta
from hysds.celery import app
import requests
import json
import elasticsearch

# Setup logger for this job here. Should log to STDOUT or STDERR as this is a job
# logging.basicConfig(level=logging.DEBUG)
# LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

es_url = app.conf["GRQ_ES_URL"]
_index = None
_type = None
ES = elasticsearch.Elasticsearch(es_url)


def get_index_and_type():
    """
    Get the index and type to use in ES operations
    :return:
    """
    try:
        dataset = json.loads(open("dataset.json", "r").read())
        ipath = dataset[0].get("ipath")
        typ = ipath[ipath.rfind("/"):]
        version = dataset[0].get("version")
        index = "grq_v{}_acquisition-bos_sarcat".format(version)
    except:
        index = "grq_v0.2_acquisition-bos_sarcat"
        typ = "acquisition-BOS_SARCAT"
    return index, typ


def delete_document_by_id(index, typ, acq_id):
    """
    Delete documents in ES by ID
    :param index:
    :param typ:
    :param acq_id:
    :return:
    """
    try:
        ES.delete(index=index, doc_type=typ, id=acq_id)
    except:
        raise Exception("Failed to delete document {} from index {}".format(acq_id, index))


def query_ES_acqs_to_delete(query, es_index):
    """
    This function creates a list of acquisition IDs that are in the past.
    :param query:
    :param es_index:
    :return:
    """
    acq_list = []
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search?search_type=scan&scroll=60&size=10000".format(rest_url)
    if es_index:
        url = "{}/{}/_search?search_type=scan&scroll=60&size=10000".format(rest_url, es_index)
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    # logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if count == 0:
        return []
    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" % query)
        return []
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0:
            break
        hits.extend(res['hits']['hits'])

    for item in hits:
        acq_id = item.get("_id")
        try:
            start_time = datetime.strptime(item.get("fields").get("starttime")[0], "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            start_time = datetime.strptime(item.get("fields").get("starttime")[0], "%Y-%m-%dT%H:%M:%SZ")

        """
        Check if acquisition is older than 2 days.
        We are maintaining a bit of padding (2 days), in case
        we want to verify if something predicted has actually
        come in as acquired or not.
        """
        if start_time < datetime.now() - timedelta(days=2):
            print("ID: {}  Start time: {}".format(acq_id, start_time))
            acq_list.append(acq_id)

    return acq_list


def query_acqs_to_delete(index, status):
    """
    find acquisitions with a specific status
    :param index:
    :param status:
    :return:
    """
    query = {
      "query": {
        "bool": {
          "must": {
            "match": {
              "metadata.status": status
            }
          }
        }
      },
      "fields": ["_id",
                 "starttime",
                 "endtime"]
    }

    return query_ES_acqs_to_delete(query=query, es_index=index)


def get_past_acqs(acquisitions):
    acqs = copy.deepcopy(acquisitions)
    for acq in acqs:
        try:
            start_time = datetime.strptime(acq.get("_source").get("starttime"), "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            start_time = datetime.strptime(acq.get("_source").get("starttime"), "%Y-%m-%dT%H:%M:%SZ")

        if start_time > datetime.now() - timedelta(days=2):
            acqs.remove(acq)
    return acqs


def delete_past_acqs(acquisitions):
    """
    deletes acquisition given a list of acquisistion ids
    :param acquisitions:
    :return:
    """
    if len(acquisitions) != 0:
        for acq in acquisitions:
            print("Deleting ID: {}".format(acq))
            delete_document_by_id(index=_index, typ=_type, acq_id=acq)
    return


if __name__ == "__main__":
    '''
    Main program to delete  planned or predicted
    acquisitions from bos sarcat acq index
    '''
    _index, _type = get_index_and_type()

    # find and delete past planned acquisitions
    planned = query_acqs_to_delete(index=_index, status="PLANNED")
    if len(planned) != 0:
        print("Deleting following PLANNED acquisitions :::")
        delete_past_acqs(acquisitions=planned)
    else:
        print("No planned acquisitions to delete")

    # find and delete past predicted acquisitions
    predicted = query_acqs_to_delete(index=_index, status="PREDICTED")
    if len(predicted) != 0:
        delete_past_acqs(acquisitions=predicted)
    else:
        print("No predicted acquisitions to delete")

