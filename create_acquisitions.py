from builtins import str
from builtins import range
import logging
import os
import shutil
from hysds.celery import app
import hysds.orchestrator
from hysds.dataset_ingest import ingest
import json
import copy
import re
import requests
from shapely.geometry import shape
import subprocess
from string import Template
from datetime import datetime
import geojson
import shapely.wkt
import traceback

# Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format,level=logging.INFO)
LOGGER = logging.getLogger("hysds")


'mappings created for metadata values'
##################################################################
DATASET_VERSION = "v2.0"
DEFAULT_ACQ_TYPE = "NOMINAL"
# the below are all templates being used
# these may need to edited later on for every platform
# only sentinel can be downloaded from scihub
SCIHUB_URL_TEMPLATE = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/"
SCIHUB_DOWNLOAD_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/$value"
ICON_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/Products('Quicklook')/$value"

PLATFORM_NAME = { "S1": "Sentinel-1" }

INSTRUMENT_NAME = {
    "Sentinel-1A": "Synthetic Aperture Radar (C-band)",
    "Sentinel-1B": "Synthetic Aperture Radar (C-band)",
    "TerraSAR-X-1": "Synthetic Aperture Radar (X-band)",
    "TanDEM-X-1": "Digital Elevation Model",
    "ALOS-2": "Phased-Array L-band Synthetic Aperture Radar - 2"
}

INSTRUMENT_SHORT_NAME = {
    "Sentinel-1A": "SAR-C SAR",
    "Sentinel-1B": "SAR-C SAR",
    "TerraSAR-X-1": "SAR-X SAR",
    "TanDEM-X-1": "TanDEM",
    "ALOS-2": "PALSAR-2"
}
##################################################################


def get_existing_acqs(start_time, end_time, location=False):
    """
    This function would query for all the acquisitions that
    temporally and spatially overlap the scrape in the current run.
    :param location:
    :param start_time:
    :param end_time:
    :return:
    """
    index = "grq_{}_acquisition-bos_sarcat".format(DATASET_VERSION)

    query = {
      "query": {
        "filtered": {
          "query": {
            "bool": {
              "must": [
                {
                  "range": {
                    "metadata.sensingStart": {
                      "to": end_time,
                      "from": start_time
                    }
                  }
                }
              ]
            }
          }
        }
      }
    }

    if location:
        geo_shape = {
          "geo_shape": {
            "location": {
              "shape": location
            }
          }
        }
        query["query"]["filtered"]["filter"] = geo_shape

    acq_ids = []
    rest_url = app.conf["GRQ_ES_URL"][:-1] if app.conf["GRQ_ES_URL"].endswith('/') else app.conf["GRQ_ES_URL"]
    url = "{}/{}/_search?search_type=scan&scroll=60&size=10000".format(rest_url, index)
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    if count == 0:
        return []
    if '_scroll_id' not in scan_result:
        LOGGER.info("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" % query)
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
        acq_ids.append(item.get("_id"))

    return set(acq_ids)


def ingest_acq_dataset(starttime, endtime, ds_cfg ="/home/ops/verdi/etc/datasets.json"):
    """
    Ingest acquisition dataset
    :param starttime:
    :param endtime:
    :param ds_cfg: path to datasets.json

    TO DO: (somewhat done)
           Change the ingestion mechanism similar to Scihub acquisition ingest
           Create a tmp dir and make dataset dir inside that
           After ingestion delete the tmp dir

           Background: tried to do that but when searching for dir names
           starting with 'acquisition-' but didn't find any in the tempdir.
           So nothing was being ingested. I reverted changes from commits:
           bd2c26eb2c8ec66e7ec4785b86ce77a65b4394ff
           e82f94a6103f38db5b07f1cd1a2e5766b3be0d00
           9d6e17c121b024e8f64eafbe86bba0d82df14202
    :return:
    """

    existing = get_existing_acqs(starttime, endtime)
    'for every folder staring with `acquisition-` call ingest'
    acq_dirs = [x for x in os.listdir('.') if x.startswith('acquisition-')]

    total_ingested = 0
    total_ingest_failed = 0
    total_existing = 0
    failed_publish = []
    for dir in acq_dirs:
        if os.path.isdir(dir):
            acq_id = dir
            abspath_dir = os.path.abspath(acq_id)
            if dir not in existing:
                try:
                    ingest(acq_id, ds_cfg, app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, abspath_dir, None)
                    LOGGER.info("Successfully ingested dataset {}".format(acq_id))
                    shutil.rmtree(acq_id)
                    total_ingested += 1
                except Exception as e:
                    LOGGER.error("Failed to ingest dataset {}".format(acq_id))
                    LOGGER.error("Exception: {}".format(e))
                    failed_publish.append(acq_id)
                    total_ingest_failed += 1
            else:
                LOGGER.info("acquisition found in existing, will delete directory: %s" % acq_id)
                shutil.rmtree(acq_id)
                total_existing += 1
    LOGGER.info('#' * 100)
    LOGGER.info('total ingested: %i' % total_ingested)
    LOGGER.info('total existing: %i' % total_existing)
    LOGGER.info('total ingest failed: %i' % total_ingest_failed)
    LOGGER.info('list of failed ingests: {}'.format(failed_publish))
    LOGGER.info('#' * 100)
    return


def is_outdated(existing_ingestion_time, new_ingestion_time):
    """
    When we do scrapes over a long period of time we would end up creating duplicate predicted acqs
    this is detected when a dataset already exists in the work dir with same ID
    this function checks based on bos ingestion timestamp which acquisition is the latest
    :param existing_ingestion_time:
    :param new_ingestion_time:
    :return:
    """
    LOGGER.info("Existing: {}".format(existing_ingestion_time))
    LOGGER.info("New: {}".format(new_ingestion_time))
    try:
        existing = datetime.strptime(existing_ingestion_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        existing = datetime.strptime(existing_ingestion_time, '%Y-%m-%dT%H:%M:%SZ')

    try:
        new = datetime.strptime(new_ingestion_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        new = datetime.strptime(new_ingestion_time, '%Y-%m-%dT%H:%M:%SZ')

    if new > existing:
        return True
    else:
        return False


def get_download_url(alt_identifier):
    """
    Construct download url from template using the alt_identifier in metadata

    TO DO: The default is to use the scihub download template to populate this value
           But since this is a multi sensor ingest, update/ add more download url
           templates for endpoints other than SciHub. The information is not in the
           BOS metadata so will need to do some research.
    :param alt_identifier:
    :return:
    """
    if alt_identifier is not None:
        return Template(SCIHUB_DOWNLOAD_URL).safe_substitute(id=alt_identifier)
    else:
        return None


def get_sensor_file_name(sensor):
    """
    Extract out sensor name
    Sometimes bos metadata has them within parenthesis
    :param sensor:
    :return:
    """
    if "(" in sensor:
        sensor = sensor[sensor.find("(") + 1:sensor.find(")")]
    sensor = sensor.replace(" ", "_")

    return sensor


def make_clockwise(coords):
    """
    returns the coordinates in clockwise direction. takes in a list of coords.
    :param coords:
    :return:
    """
    if get_area(coords) > 0:
        coords = coords[::-1] # switch direction if not clockwise
    return coords


def is_anti_meridian(es_json):
    """
    Checking here if the polygon crosses the time meridian
    If so then correct it
    :param es_json:
    :return:
    """
    positive = 0
    negative = 0
    for coord in es_json["coordinates"][0]:
        if float(coord[0]) < 0:
            negative += 1
        else:
            positive += 1
    if negative == 0 or positive == 0:
        return False
    else:
        return True


def get_area(coords):
    """
    get area of enclosed coordinates- determines clockwise or counterclockwise order
    :param coords:
    :return:
    """
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    return area / 2.0


def get_polygon(wkt_polygon):
    """
    Return geojson given a polygon in WKT format
    :param wkt_polygon:
    :return:
    """
    wkt_geom = shapely.wkt.loads(wkt_polygon)
    polygon = geojson.Feature(geometry=wkt_geom, properties={})
    return polygon.geometry


def get_wkt(geojson_polygon):
    """
    Return wKT formated polygon given geojson
    :param geojson_polygon:
    :return:
    """
    g1 = geojson.loads(geojson_polygon)
    g2 = shape(g1)
    return g2.wkt


def get_gml(geo_json):
    """
    Get polygon in GML format given GeoJSON
    :param geo_json:
    :return:
    """
    gml = "<gml:Polygon srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\" " \
          "xmlns:gml=\"http://www.opengis.net/gml\">\n  " \
          " <gml:outerBoundaryIs>\n      <gml:LinearRing>\n         <gml:coordinates>"

    coordinates = geo_json["coordinates"][0]
    for point in coordinates:
        gml = gml + str(point[1]) + "," + str(point[0]) + " "
    gml = gml[:-1] + "</gml:coordinates>\n      </gml:LinearRing>\n   </gml:outerBoundaryIs>\n</gml:Polygon>"
    return gml


def valid_es_geometry(geometry):
    """
    transform the geoJSON to ES acceptable form
    The following steps are taken to sanitize and correct the geojson:
    1. Make sure the geojson points are in the form ABCDA
       The starting and ending point should be the same,
       other than that no other points should be repeated
    2. Make sure the geojson is in a clockwise order
    3. Handle anti-meridian cases
       The polygons crossing the prime meridian line need to be correcte
       such that the polygon overlaps with the prime meridian line instead
       of spanning all the way across the world.
    4. Handle bow-tie polygons.
       This is the case where the polygon is in the form ADBCA
       instead of ABCDA. These polygons need to corrected to the
       expected format.
    :param geometry: polygon in GeoJSON format
    :return: corrected GeoJSON (maybe the same as original)


    TO DO:
          Implement scenario 4 and 3
    """
    es_json = copy.deepcopy(geometry)

    # now for any meridian crossing polygons, add 360 to longitude
    # less than 0, since ES can handle 0 to 360
    # if is_anti_meridian(es_json):
    #     index = 0
    #     for coord in es_json["coordinates"][0]:
    #         if float(coord[0]) < 0:
    #             es_json["coordinates"][0][index][0] = float(coord[0]) + 360
    #         index +=1

    size = len(es_json["coordinates"][0])

    if es_json["coordinates"][0][size - 1] == es_json["coordinates"][0][size - 2]:
        del es_json["coordinates"][0][size - 1]

    es_json["coordinates"][0] = make_clockwise(es_json["coordinates"][0])
    return es_json


def not_raw(product_name):
    """
    Check if the product is of type RAW
    we check for this because we are not interested in
    creation and ingestion of a RAW dataset.
    :param product_name:
    :return:
    """
    match = re.search(r'([\w.-]+)_([\w.-]+)_([\w.-]+)__([\d])([\w])([\w.-]+)', product_name)
    if match:
        product_type = match.group(3)

        if product_type == "RAW":
            return False
        else: return True
    else:
        return True


def past_planned(status, start_time, end_time):
    if status == "planned":
        try:
            start_datetime = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            start_datetime = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')

        try:
            end_datetime = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            end_datetime = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')

        now = datetime.now()
        if start_datetime < now and end_datetime < now:
            return True
        else:
            return False
    else:
        return False


def get_future_predicted_dates(predicted_dates):
    """
    From the predicted dates list provided by BOS
    filter out the dates that are in the past.
    This won't be an issue for keep up jobs but when we catch-up
    or gap fills we are not interested in creating acquisitions
    from the past with status PREDICTED.
    :param predicted_dates:
    :return: list of predicted dates in future
    """
    dates_set = copy.deepcopy(predicted_dates)
    future_dates_set = list()
    date_today = datetime.now().strftime("%Y-%m-%d")
    for date in dates_set:
        if date >= date_today:
            future_dates_set.append(date)
    return future_dates_set


def make_dataset_file(product_name, record, starttime = None, endtime = None):
    """
    This function creates the dataset.json file for the acquisition dataset
    :param product_name:
    :param record:
    :param starttime:
    :param endtime:
    :return:
    """
    folder_name = product_name
    dataset = dict()

    if starttime is None and endtime is None:
        dataset["endtime"] = record["properties"]["stop_time"]
        dataset["starttime"] = record["properties"]["start_time"]
    else:
        dataset["starttime"] = starttime
        dataset["endtime"] = endtime

    dataset["label"] = product_name
    dataset["location"] = valid_es_geometry(record["geometry"])
    dataset["version"] = DATASET_VERSION

    dataset_file = open("%s/%s.dataset.json" % (folder_name, product_name), 'w')
    dataset_file.write(json.dumps(dataset))
    dataset_file.close()

    return


def get_platform_name(mission):
    """
    Set the platform name
    :param mission:
    :return:
    """
    # if the mission name is in the mapping declared in beginning of script, use mapping
    if mission in PLATFORM_NAME:
        platform_name = PLATFORM_NAME[mission]
    else:
        # if not the set the platform_name the same as the mission name
        platform_name = mission

    return platform_name


def get_instrument_name(satellite):
    """
    Set and return the instrument name and short name
    :param satellite:
    :return: instrument name and instrument short name
    """
    instrument_name = satellite
    instrument_short_name = satellite
    """
    if satellite name is in INSTRUMENT_NAME and INSTRUMENT_SHORT_NAME mapping
    then use value from there
    other wise use the satellite name for both values
    """
    if satellite in INSTRUMENT_NAME:
        instrument_name = INSTRUMENT_NAME[satellite]
    if satellite in INSTRUMENT_SHORT_NAME:
        instrument_short_name = INSTRUMENT_SHORT_NAME[satellite]

    return instrument_name, instrument_short_name


def get_platform_identifier(satellite_name, satellite_id):
    """
    set the satellite_id
    :param satellite_name:
    :param satellite_id:
    :return:
    """
    return satellite_id


def get_start_timestamp(start_time):
    """
    get start timestamp to use in datasetname
    It removes the hyphens and colons from the timestamp
    :param start_time:
    :return:
    """
    start_time = start_time.replace("-","")
    start_time = start_time.replace(":","")
    return start_time


def get_product_class(satellite_name, product_name):
    """
    Set Product Class, processing level and product type from the acquisition identifier
    Only being done for Sentinel 1A and 1B acquisitions
    For others, they are set to None

    TO DO: Currently this is done only for Sentinel acquisitions because we have domain knowledge
           on how to extract this information. But I don't know if these concepts exist for other
           satellites. Need to research. If they exist, then need to implement.
    :param satellite_name:
    :param product_name:
    :return: product_class, product_type, processing_level
    """

    product_type = None
    processing_level = None
    product_class = None

    if satellite_name == "Sentinel-1A" or satellite_name == "Sentinel-1B":
        # sample name of S1 file : S1A_IW_SLC__1SDV_20150909T163711_20150909T163746_007640_00A97E_A69D
        match = re.search(r'([\w.-]+)_([\w.-]+)_([\w.-]+)__([\d])([\w])([\w.-]+)', product_name)
        if match:
            product_type = match.group(3)
            processing_level = match.group(4)
            product_class = match.group(5)

    return product_class, product_type, processing_level


def make_met_file(product_name, record):
    """
    Creation of the met file
    The fields in this file contain as much metadata as possible
    The field names and some values have been reflected using what
    we see in the acquisition metadata from ESA.
    This was done to maintain some similarity in the metadata
    structure across all acquisition types in our archive
    :param product_name:
    :param record:
    :return:
    """

    metadata = dict()

    metadata["acquisitiontype"] = DEFAULT_ACQ_TYPE

    if record["properties"]["alt_identifier"] is not None:
        metadata["alternative"] = Template(SCIHUB_URL_TEMPLATE)\
            .safe_substitute(id=record["properties"]["alt_identifier"])

    # the archived file is usually of zip format.
    metadata["archive_filename"] = record["properties"]["identifier"] + ".zip"

    # set acquisition location and bounding box
    metadata["location"] = valid_es_geometry(record["geometry"])
    metadata["bbox"] = metadata["location"]["coordinates"][0]

    # set orbit direction
    if record["properties"]["flight_direction"] == "A":
        direction = "asc"
    elif record["properties"]["flight_direction"] == "D":
        direction = "dsc"
    else:
        direction = "N/A"
    metadata["direction"] = direction
    metadata["look_direction"] = record["properties"]["look_direction"]

    metadata["browse_url"] = record["properties"]["browse_url"]



    """
    storing the bbox that sarcat originally provides. The bbox set previously is
    sanitized to what we use internally
    """
    metadata["sarcat_bbox"] = record["properties"]["bbox"]
    metadata["download_url"] = get_download_url(record["properties"]["alt_identifier"])

    # the file is usually of SAFE format.
    metadata["filename"] = record["properties"]["identifier"] + ".SAFE"
    metadata["format"] = "SAFE"

    # set gml and wkt footprints for acquisition
    metadata["footprint"] = get_wkt(json.dumps(record["geometry"]))
    metadata["gmlfootprint"] = get_gml(record["geometry"])

    # setting link to icon from a template
    # TODO: this is deduced from ESA metadata. Needs to be updated for other platforms
    metadata["icon"] = Template(ICON_URL).safe_substitute(id=record["properties"]["alt_identifier"])

    # set identifiers
    metadata["id"] = record["properties"]["alt_identifier"]
    metadata["identifier"] = record["properties"]["identifier"]

    # store ingestion time
    metadata["bos_ingestion_time"] = record["properties"]["bos_ingest"]

    # set satellite and agency information
    metadata["satellite_id"] = record["properties"]["satellite_id"]
    metadata["agency"] = record["properties"]["agency"]
    metadata["launch_date"] = record["properties"]["launch_date"]
    metadata["eol_date"] = record["properties"]["eol_date"]
    metadata["instrumentname"], metadata["instrumentshortname"] = \
        get_instrument_name(record["properties"]["satellite_name"])
    metadata["platform"] = record["properties"]["satellite_name"]
    metadata["platformidentifier"] = get_platform_identifier(record["properties"]["satellite_name"],
                                                             record["properties"]["satellite_id"])
    metadata["platformname"] = get_platform_name(record["properties"]["mission"])
    metadata["polarisationmode"] = record["properties"]["polarization"]
    metadata["productclass"], metadata["producttype"], metadata["processing_level"] = \
        get_product_class(record["properties"]["satellite_name"], product_name)

    # set how many days this satellites repeat cycle is and orbit information
    metadata["repeat_cycle"] = record["properties"]["repeat_cycle"]
    metadata["lastorbitnumber"] = record["properties"]["absolute_orbit"]
    metadata["lastrelativeorbitnumber"] = record["properties"]["relative_orbit"]
    metadata["missiondatatakeid"] = record["properties"]["scene_id"]
    metadata["orbitNumber"] = record["properties"]["absolute_orbit"]
    metadata["sensoroperationalmode"] = record["properties"]["beam_mode"]
    metadata["slicenumber"] = record["properties"]["frame"]

    # TODO: update query api value based on the platform type
    metadata["query_api"] = "opensearch"
    metadata["sensingStart"] = record["properties"]["start_time"]
    metadata["sensingStop"] = record["properties"]["stop_time"]

    if record["properties"]["status"].upper() == "ARCHIVED":
        """
        We are using the term ACQUIRED on purpose as acquisitions we get
        from BOS implies that the acquisition has be acquired by some sensor
        but we don't guarantee the status regarding whether the acquisition is
        available to download
        
        The status ACQUIRED is used by the scihub scraper as it guarantees the 
        availability and location of the acquisition metadata and SLC
        """
        metadata["status"] = "ACQUIRED"
    else:
        metadata["status"] = record["properties"]["status"].upper()

    # brief summary with key
    metadata["summary"] = "Date: " + metadata["sensingStart"] + ", Instrument: " + metadata[
        "platformname"] + ", Mode: " + metadata["polarisationmode"] + ", Satellite: " + metadata[
                              "platform"] + ", Size: "
    metadata["swathidentifier"] = record["properties"]["beam_swath"]

    # the original identifier of the acquisition as set by source agency
    metadata["title"] = record["properties"]["identifier"]

    metadata["track_number"] = record["properties"]["relative_orbit"]

    # the unique identifier of the acquisition as set by source agency
    metadata["uuid"] = record["properties"]["alt_identifier"]

    # setting the source of scraped acquisition to BOS SARCAT
    metadata["source"] = "bos-sarcat"


    # create acquisition dataset ID according to our internal naming convention
    # acquisition-[platform]_[starttime]_[track_number]_[mode]-bos_sarcat
    folder_name = "acquisition-" + str(metadata["platform"]) + "_" \
                  + str(get_start_timestamp(metadata["sensingStart"])) + "_" \
                  + str(metadata["track_number"]) + "_"\
                  + str(get_sensor_file_name(metadata["sensoroperationalmode"]))\
                  + "-bos_sarcat"

    """
    if the acquisition status is planned then append '-planned' to the dataset name
    acquisition-[platform]_[starttime]_[track_number]_[mode]-bos_sarcat-planned
    """
    if metadata["status"] == "PLANNED":
        folder_name += "-planned"

    dataset_name = folder_name

    """
    During a scrape we may receive different versions of the same acquisition
    We want to make sure that we only ingest the latest version
    To do so we compare the ingestion timestamp of the datset already existing
    in the work directory vs. the new acquisition we are currently dealing with.
    
    If the current acquisition is newer then remove folder of older one and
    proceed with creation of dataset
    
    Else, you will see an exception in trying to create a dataset folder with the
    same acquisition name.
    
    
    TO DO: Move the try catch block of dataset creation in the IF block, that way
           won't attempt a dataset creation and fail to create folder.
    """
    if os.path.isdir(folder_name):
        LOGGER.info("Folder already exists with name: {}".format(folder_name))
        LOGGER.info("Comparing the ingest times of existing vs new")
        existing = json.loads(open("{}/{}.met.json".format(folder_name,dataset_name, "r")).read())
        if is_outdated(existing_ingestion_time=existing["bos_ingestion_time"], new_ingestion_time=metadata["bos_ingestion_time"]):
            LOGGER.info("New dataset is more recent. Removing older dir.")
            shutil.rmtree(folder_name)
        else:
            LOGGER.info("Keeping existing dataset. Expect to see exception message.")
    try:
        LOGGER.info("Creating Dataset for {}".format(folder_name))
        os.makedirs(folder_name, 0o777)
        met_file = open("%s/%s.met.json" % (folder_name, dataset_name), 'w')
        met_file.write(json.dumps(metadata))
        met_file.close()
    except Exception as ex:
        LOGGER.error("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))

    return folder_name


def make_predicted_datasets(record):
    """
    Creating the dataset for predicted acquisitions
    This contains a subset of the metadata fields as seen in acquired acquisitions
    :param record:
    :return:
    """
    predicted_dates = sorted(record["properties"]["predicted"].split(","))
    predicted_dates = get_future_predicted_dates(predicted_dates)
    orbit_increment = 0 # this count is maintained to calculate the consecutive orbit number
    for date in predicted_dates:
        metadata = dict()
        orbit_increment +=1 # increase orbit number
        metadata["acquisitiontype"] = DEFAULT_ACQ_TYPE
        metadata["location"] = record["geometry"]
        metadata["bbox"] = metadata["location"]["coordinates"][0]
        if record["properties"]["flight_direction"] == "A":
            direction = "asc"
        elif record["properties"]["flight_direction"] == "D":
            direction = "des"
        else:
            direction = "N/A"
        metadata["direction"] = direction
        metadata["look_direction"] = record["properties"]["look_direction"]
        metadata["satellite_id"] = record["properties"]["satellite_id"]
        metadata["agency"] = record["properties"]["agency"]
        metadata["launch_date"] = record["properties"]["launch_date"]
        metadata["eol_date"] = record["properties"]["eol_date"]
        metadata["repeat_cycle"] = record["properties"]["repeat_cycle"]
        metadata["sarcat_bbox"] = record["properties"]["bbox"]
        metadata["footprint"] = get_wkt(json.dumps(record["geometry"]))
        metadata["gmlfootprint"] = get_gml(record["geometry"])
        metadata["id"] = None
        metadata["identifier"] = None
        metadata["bos_ingestion_time"] = record["properties"]["bos_ingest"]
        metadata["instrumentname"], metadata["instrumentshortname"] = \
            get_instrument_name(record["properties"]["satellite_name"])
        metadata["missiondatatakeid"] = record["properties"]["scene_id"]
        metadata["orbitNumber"] = str(int(record["properties"]["absolute_orbit"]) + orbit_increment)
        metadata["platform"] = record["properties"]["satellite_name"]
        metadata["platformidentifier"] = get_platform_identifier(record["properties"]["satellite_name"],
                                                                 record["properties"]["satellite_id"])
        metadata["platformname"] = get_platform_name(record["properties"]["mission"])
        metadata["polarisationmode"] = record["properties"]["polarization"]

        """
        Creating the start and end times for the predicted acquisitions
        the timestamp = predicted date + "T" + time from original acquisition
        """
        start_time = record["properties"]["start_time"]
        metadata["sensingStart"] = date + start_time[start_time.find("T"):]
        stop_time = record["properties"]["stop_time"]
        metadata["sensingStop"] = date + stop_time[stop_time.find("T"):]

        metadata["sensoroperationalmode"] = record["properties"]["beam_mode"]
        metadata["status"] = "PREDICTED"
        metadata["track_number"] = record["properties"]["relative_orbit"]
        metadata["source"] = "bos-sarcat"
        metadata['processing_version'] = None

        folder_name = "acquisition-" + str(metadata["platform"]) + "_" \
                      + str(get_start_timestamp(metadata["sensingStart"])) + "_" \
                      + str(metadata["track_number"]) + "_" \
                      + str(get_sensor_file_name(metadata["sensoroperationalmode"])) \
                      + "-bos_sarcat-predicted"
        dataset_name = folder_name

        """
        This is the same as the dataset creation done for acquired acquisitions
        TO DO: Make this a function, reduce redundancy in code
        """
        if os.path.isdir(folder_name):
            LOGGER.info("Folder already exists with name: {}".format(folder_name))
            LOGGER.info("Comparing the ingest times of existing vs new")
            existing = json.loads(open("{}/{}.met.json".format(folder_name, dataset_name, "r")).read())
            if is_outdated(existing_ingestion_time=existing["bos_ingestion_time"], new_ingestion_time=metadata["bos_ingestion_time"]):
                LOGGER.info("New dataset is more recent. Removing older dir.")
                try:
                    shutil.rmtree(folder_name)
                except Exception as ex:
                    LOGGER.error("Failed to delete dataset for {}. Because {}. {}".format(folder_name, ex.message,
                                                                                   traceback.format_exc()))
            else:
                LOGGER.info("Keeping existing dataset. Expect to see exception message.")

        try:
            LOGGER.info("Creating Dataset for {}".format(folder_name))
            os.makedirs(folder_name, 0o777)
            met_file = open("%s/%s.met.json" % (folder_name, dataset_name), 'w')
            met_file.write(json.dumps(metadata))
            met_file.close()
            make_dataset_file(dataset_name, record, metadata["sensingStart"], metadata["sensingStop"])
        except Exception as ex:
            LOGGER.error("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message,
                                                                                 traceback.format_exc()))


def create_product(record):
    """
    For every acquistion from BOS, create an archived / planned acquistions
    If the predicted dates are provided then create the predicted acquisitions
    :param record:
    :return:
    """
    product_name = record["properties"]["identifier"] # original acq name
    status = record["properties"]["status"] # status of acquisition
    start_time = record["properties"]["start_time"] # start time of acquisition
    end_time = record["properties"]["stop_time"] # end time of acquisition

    # Only create dataset if it is not RAW and not a planned acq from the past
    if not_raw(product_name) and not past_planned(status, start_time, end_time):
        # create the met file and get the our internal dataset ID
        product_name = make_met_file(product_name, record)
        make_dataset_file(product_name, record)
        if "predicted" in record["properties"]:
            if record["properties"]["predicted"] is not None:
                LOGGER.info("Working with bos product : {}".format(product_name))
                make_predicted_datasets(record)


def main():
    """
    This script calls the bos scrape command
    Parses the output JSON and creates planned, predicted and acquired acquisitions
    The ingestion of the acquisition is handled inline by called the hysds.dataset_ingest.ingest
    :return:
    """
    try:
        """
        Constructing the command line call
        Read the inputs from _context.json and 
        create the positional arguments accordingly
        """

        'get values from _context.json and create the positional args for the call to bos_sarcat command line tool'
        get_cmd = "bos_sarcat_scraper"
        context = open("_context.json", "r")
        ctx = json.loads(context.read())

        if ctx.get('bos_ingest_time') != '' and ctx.get('bos_ingest_time') is not None:
            # if we use bos_ingest_time then we can assume we dont need to use start_time and end_time
            # starttime and endtime to find existing acquisitions in ES
            bos_ingest_last = starttime = ctx["bos_ingest_time"]
            get_cmd += ' --fromBosIngestTime ' + bos_ingest_last
            endtime = "{}Z".format(datetime.now().isoformat())
        else:
            # we'll assume to use start_time and end_time if we dont use bis_ingest_time
            # starttime and endtime to find existing acquisitions in ES
            from_time = starttime = ctx['from_time']
            end_time = endtime = ctx['end_time']
            get_cmd += ' --fromTime ' + from_time
            get_cmd += ' --toTime ' + end_time

        get_cmd += " --sortBy bos_ingest --sort des" # by default asking for results to be sorted in descending order
        LOGGER.info('#' * 100)
        LOGGER.info('bos_sarcat_scraper command:')
        LOGGER.info(get_cmd)
        LOGGER.info('#' * 100)

        # call the bos commandline tool
        p = subprocess.Popen(get_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = str(p.communicate())

        # filter out the JSON response from other log messages in output
        start_pos = output.find("{")
        end_pos = output.rfind("}")
        LOGGER.info(output)
        results = json.loads(output[start_pos:end_pos+1])

        "Iterate through the acquisitions to create and ingest them."
        LOGGER.info("Number of acquisitions: %s" % results["totalFeatures"])
        if int(results["totalFeatures"]) != 0:
            for result in results["features"]:
                # Create acquisition dataset for every acquisition
                create_product(result)
            # Ingest all created acquisitions, it only ingests the delta
            ingest_acq_dataset(starttime, endtime)
        else:
            LOGGER.info("No new SAR acquisitions since last scrape.")
            return
    except Exception as e:
        "Pipe out exceptions to error and traceback file so it's visible on Figaro"
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    return


if __name__ == "__main__":
    main()
