import requests
import logging
import json
import datetime
import os
from string import Template
from shapely.geometry import shape
import geojson

#Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
BASE_PATH = os.path.dirname(__file__)

param_mapping = {"fromTime":
                     {"param_name":"start_time",
                      "param_operator":">="
                      },
                 "toTime":
                     {"param_name":"stop_time",
                      "param_operator":"<="
                      },
                 "fromBosIngestTime":
                     {"param_name":"bos_ingest",
                      "param_operator":">="},
                 "spatialExtent":
                     {"param_name":"INTERSECTS(footprint,$shape)",
                      "param_operator":None},
                 "sortBy":
                     {"param_name": "sortBy",
                      "param_operator":"="},
                 }

def get_wkt(geojson_polygon):
    g1 = geojson.loads(json.dumps(geojson_polygon))
    g2 = shape(g1)
    return g2.wkt

def make_api_call(parameters):
    geo_server_url = "http://portal.bostechnologies.com:8080/geoserver/bos/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=bos:sarcat&outputFormat=application%2Fjson"

    #print parameters
    if parameters != {}:
        url = geo_server_url
        cql_filter = "&CQL_FILTER="
        add_param = ""
        for key in parameters:
            if key == "sortBy":
                add_param = add_param + "&" + param_mapping[key]["param_name"] + str(param_mapping[key]["param_operator"]) + str(parameters[key])
                if "sort" in parameters:
                    if parameters["sort"] == "des":
                        add_param = add_param + "+D"
            if key == "spatialExtent":
                value = get_wkt(parameters[key])
                cql_param = Template(param_mapping[key]["param_name"]).safe_substitute(shape= value)
                cql_filter = cql_filter + cql_param + "+AND+"
            else:
                if type(parameters[key]) == datetime.datetime:
                    value = datetime.datetime.strftime(parameters[key], "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    value = str(parameters[key])
                if key != "sortBy" and key != "sort":
                    cql_filter = cql_filter + str(param_mapping[key]["param_name"]) + str(param_mapping[key]["param_operator"]) + value + "+AND+"

        url = url + add_param

        if cql_filter != "&CQL_FILTER=":
            url = url + cql_filter
            # remove the extra +AND+ at the end of the url
            url = url[:url.rfind("+AND+")]
    else:
        url = geo_server_url

    print("making request with url : %s" % url)
    LOGGER.info("making request with url : %s" % url)


    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        return json.dumps(r.json())
    else:
        LOGGER.debug("Bos SAR CAT call unsuccessful")
        raise Exception("API call to Bos SAR CAT failed")