#!/usr/bin/env python

'''
This script is for purging planned and predicted
acquisitions in sarcat index that occured 2 days ago
'''

import os
import logging
import copy
from datetime import datetime, timedelta
from hysds.celery import app
import json
import elasticsearch

#Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
# logging.basicConfig(level=logging.DEBUG)
# LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

es_url = app.conf['GRQ_ES_URL']
_index = None
_type = None
ES = elasticsearch.Elasticsearch(es_url)

def get_index_and_type():
    try:
        dataset = json.loads(open("dataset.json", "r").read())
        ipath = dataset[0].get("ipath")
        typ = ipath[ipath.rfind("/"):]
        version = dataset[0].get("version")
        index = "grq_v{}_acquisition-sarcat".format(version)
    except:
        index = "grq_{}_acquisition-sarcat".format("v0.2")
        typ = "acquisition-SARCAT"
    return index, typ

def delete_document_by_id(index, type, id):
    """
    Delete documents in ES by ID
    :param id:
    :return:
    """
    ES.delete(index=index, doc_type=type, id=id)

def query_ES(query ,es_index):
    result = ES.search(index=es_index, body=query, request_timeout=30, size=10000)
    return result


def query_acqs_to_delete(index, type, status):
    """
    find acquisitions with a status
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
      }
    }
    return query_ES(query=query, es_index=index)

def get_past_acqs( acquisitions ):
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
    if int(acquisitions.get("hits").get("total")) != 0:
        past_acqs = get_past_acqs(acquisitions.get("hits").get("hits"))
        for acq in past_acqs:
            print ("ID: {}  Start time: {}".format(acq.get("_id"), acq.get("_source").get("starttime")))
            delete_document_by_id(index=_index, type=_type, id=acq.get("_id"))

if __name__ == "__main__":
    '''
    Main program to delete  planned or predicted
    acquisitions from bos sarcat acq index
    '''
    _index, _type = get_index_and_type()

    #find and delete past planned acquisitions
    planned = query_acqs_to_delete (index=_index, type=_type, status="PLANNED")
    if int(planned.get("hits").get("total")) != 0:
        print ("Deleting following PLANNED acquisitions :::")
        delete_past_acqs(acquisitions=planned)
    else:
        print "No planned acquisitions to delete"
    # find and delete past predicted acquisitions
    predicted = query_acqs_to_delete(index=_index, type=_type, status="PREDICTED")
    if int(predicted.get("hits").get("total")) != 0:
        delete_past_acqs(acquisitions=predicted)
    else:
        print "No predicted acquisitions to delete"







