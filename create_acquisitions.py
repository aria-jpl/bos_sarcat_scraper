import logging
import os
import shutil
from redis import ConnectionPool, StrictRedis
# from hysds_commons.net_utils import get_container_host_ip
import ingest_acq
import json
import copy
import re
from shapely.geometry import shape
import subprocess
from string import Template
from datetime import datetime
import geojson
import shapely.wkt
import traceback

# Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

POOL = None
DATASET_VERSION = "v0.2"
DEFAULT_ACQ_TYPE = "NOMINAL"
SCIHUB_URL_TEMPLATE = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/"
SCIHUB_DOWNLOAD_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/$value"
ICON_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/Products('Quicklook')/$value"

# if POOL is None:
#     redis_url = "redis://%s" % get_container_host_ip()
#     POOL = ConnectionPool.from_url(redis_url)
# r = StrictRedis(connection_pool=POOL)

PLATFORM_NAME = {
    "S1": "Sentinel-1"
}

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


# def get_redis_keys():
#     return r.get('last_bos_ingest_timestamp')
#
# def update_redis_key(value):
#     r.set('last_bos_ingest_timestamp', value)

def is_outdated(existing_ingestion_time, new_ingestion_time):
    print("Existing: {}".format(existing_ingestion_time))
    print("New: {}".format(new_ingestion_time))
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

def make_clockwise(coords):
    '''returns the coordinates in clockwise direction. takes in a list of coords.'''
    if get_area(coords) > 0:
        coords = coords[::-1] # switch direction if not clockwise
    return coords

def is_anti_meridian(es_json):
    """
    Checking here if the polygon crosses the time meridian
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
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    return area / 2.0

def get_polygon(wkt_polygon):
    wkt_geom = shapely.wkt.loads(wkt_polygon)
    polygon = geojson.Feature(geometry=wkt_geom, properties={})
    return polygon.geometry


def get_wkt(geojson_polygon):
    g1 = geojson.loads(geojson_polygon)
    g2 = shape(g1)
    return g2.wkt


def get_gml(geo_json):
    gml = "<gml:Polygon srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\" " \
          "xmlns:gml=\"http://www.opengis.net/gml\">\n  " \
          " <gml:outerBoundaryIs>\n      <gml:LinearRing>\n         <gml:coordinates>"

    coordinates = geo_json["coordinates"][0]
    for point in coordinates:
        gml = gml + str(point[1]) + "," + str(point[0]) + " "
    gml = gml[:-1] + "</gml:coordinates>\n      </gml:LinearRing>\n   </gml:outerBoundaryIs>\n</gml:Polygon>"
    return gml


def valid_es_geometry(geometry):
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

def not_RAW(product_name):
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
    dates_set = copy.deepcopy(predicted_dates)
    future_dates_set = list()
    date_today = datetime.now().strftime("%Y-%m-%d")
    for date in dates_set:
        if date >= date_today:
            future_dates_set.append(date)
    return future_dates_set

def make_dataset_file(product_name, record, starttime = None, endtime = None):
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
    if mission in PLATFORM_NAME:
        platform_name = PLATFORM_NAME[mission]
    else:
        platform_name = mission

    return platform_name


def get_instrument_name(satellite):
    instrument_name = satellite
    instrument_short_name = satellite
    if satellite in INSTRUMENT_NAME:
        instrument_name = INSTRUMENT_NAME[satellite]
    if satellite in INSTRUMENT_SHORT_NAME:
        instrument_short_name = INSTRUMENT_SHORT_NAME[satellite]

    return instrument_name, instrument_short_name


def get_platform_identifier(satellite_name, satellite_id):
    return satellite_id


def get_start_timestamp(start_time):
    start_time = start_time.replace("-","")
    start_time = start_time.replace(":","")
    return start_time


def get_product_class(satellite_name, product_name):
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

    metadata = dict()

    metadata["acquisitiontype"] = DEFAULT_ACQ_TYPE
    if record["properties"]["alt_identifier"] is not None:
        metadata["alternative"] = Template(SCIHUB_URL_TEMPLATE)\
            .safe_substitute(id=record["properties"]["alt_identifier"])
    metadata["archive_filename"] = record["properties"]["identifier"] + ".zip"
    metadata["location"] = valid_es_geometry(record["geometry"])
    metadata["bbox"] = metadata["location"]["coordinates"][0]
    if record["properties"]["flight_direction"] == "A":
        direction = "asc"
    elif record["properties"]["flight_direction"] == "D":
        direction = "des"
    else:
        direction = "N/A"
    metadata["direction"] = direction
    metadata["look_direction"] = record["properties"]["look_direction"]
    # metadata["browse_url"] = record["properties"]["browse_url"]
    metadata["satellite_id"] = record["properties"]["satellite_id"]
    metadata["agency"] = record["properties"]["agency"]
    metadata["launch_date"] = record["properties"]["launch_date"]
    metadata["eol_date"] = record["properties"]["eol_date"]
    metadata["repeat_cycle"] = record["properties"]["repeat_cycle"]
    metadata["sarcat_bbox"] = record["properties"]["bbox"]
    metadata["download_url"] = Template(SCIHUB_DOWNLOAD_URL).safe_substitute(id=record["properties"]["alt_identifier"])
    metadata["filename"] = record["properties"]["identifier"] + ".SAFE"
    metadata["format"] = "SAFE"
    metadata["footprint"] = get_wkt(json.dumps(record["geometry"]))
    metadata["gmlfootprint"] = get_gml(record["geometry"])
    metadata["icon"] = Template(ICON_URL).safe_substitute(id=record["properties"]["alt_identifier"])
    metadata["id"] = record["properties"]["alt_identifier"]
    metadata["identifier"] = record["properties"]["identifier"]
    metadata["bos_ingestion_time"] = record["properties"]["bos_ingest"]
    metadata["instrumentname"], metadata["instrumentshortname"] = \
        get_instrument_name(record["properties"]["satellite_name"])
    metadata["lastorbitnumber"] = record["properties"]["absolute_orbit"]
    metadata["lastrelativeorbitnumber"] = record["properties"]["relative_orbit"]
    metadata["missiondatatakeid"] = record["properties"]["scene_id"]
    metadata["orbitNumber"] = record["properties"]["absolute_orbit"]
    metadata["platform"] = record["properties"]["satellite_name"]
    metadata["platformidentifier"] = get_platform_identifier(record["properties"]["satellite_name"],
                                                             record["properties"]["satellite_id"])
    metadata["platformname"] = get_platform_name(record["properties"]["mission"])
    metadata["polarisationmode"] = record["properties"]["polarization"]
    metadata["productclass"], metadata["producttype"], metadata["processing_level"] = \
        get_product_class(record["properties"]["satellite_name"], product_name)
    metadata["query_api"] = "opensearch"
    metadata["sensingStart"] = record["properties"]["start_time"]
    metadata["sensingStop"] = record["properties"]["stop_time"]
    metadata["sensoroperationalmode"] = record["properties"]["beam_mode"]
    metadata["slicenumber"] = record["properties"]["frame"]
    if record["properties"]["status"].upper() == "ARCHIVED":
        metadata["status"] = "ACQUIRED"
    else:
        metadata["status"] = record["properties"]["status"].upper()
    metadata["summary"] = "Date: " + metadata["sensingStart"] + ", Instrument: " + metadata[
        "platformname"] + ", Mode: " + metadata["polarisationmode"] + ", Satellite: " + metadata[
                              "platform"] + ", Size: "
    metadata["swathidentifier"] = record["properties"]["beam_swath"]
    metadata["title"] = record["properties"]["identifier"]
    metadata["trackNumber"] = record["properties"]["relative_orbit"]
    metadata["uuid"] = record["properties"]["alt_identifier"]
    metadata["source"] = "bos-sarcat"

    folder_name = "acquisition-" + str(metadata["platform"]) + "_" \
                  + str(get_start_timestamp(metadata["sensingStart"])) + "_" \
                  + str(metadata["trackNumber"]) + "_"\
                  + str(metadata["sensoroperationalmode"])\
                  + "-bos_sarcat"

    if metadata["status"] == "PLANNED":
        folder_name += "-planned"

    dataset_name = folder_name

    if os.path.isdir(folder_name):
        print("Folder already exists with name: {}".format(folder_name))
        print("Comparing the ingest times of existing vs new")
        existing = json.loads(open("{}/{}.met.json".format(folder_name,dataset_name, "r")).read())
        if is_outdated(existing_ingestion_time=existing["bos_ingestion_time"], new_ingestion_time=metadata["bos_ingestion_time"]):
            print("New dataset is more recent. Removing older dir.")
            shutil.rmtree(folder_name)
        else:
            print("Keeping existing dataset. Expect to see exception message.")
    try:
        print("Creating Dataset for {}".format(folder_name))
        os.mkdir(folder_name)
        met_file = open("%s/%s.met.json" % (folder_name, dataset_name), 'w')
        met_file.write(json.dumps(metadata))
        met_file.close()
    except Exception as ex:
        print("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))
        LOGGER.warn("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))

    return folder_name


def make_predicted_datasets(record):
    predicted_dates = sorted(record["properties"]["predicted"].split(","))
    predicted_dates = get_future_predicted_dates(predicted_dates)
    orbit_increment = 0
    for date in predicted_dates:
        metadata = dict()
        orbit_increment +=1
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
        # metadata["lastorbitnumber"] = record["properties"]["absolute_orbit"]
        # metadata["lastrelativeorbitnumber"] = record["properties"]["relative_orbit"]
        metadata["missiondatatakeid"] = record["properties"]["scene_id"]
        metadata["orbitNumber"] = str(int(record["properties"]["absolute_orbit"]) + orbit_increment)
        metadata["platform"] = record["properties"]["satellite_name"]
        metadata["platformidentifier"] = get_platform_identifier(record["properties"]["satellite_name"],
                                                                 record["properties"]["satellite_id"])
        metadata["platformname"] = get_platform_name(record["properties"]["mission"])
        metadata["polarisationmode"] = record["properties"]["polarization"]
        start_time = record["properties"]["start_time"]
        metadata["sensingStart"] = date + start_time[start_time.find("T"):]
        stop_time = record["properties"]["stop_time"]
        metadata["sensingStop"] = date + stop_time[stop_time.find("T"):]
        metadata["sensoroperationalmode"] = record["properties"]["beam_mode"]
        metadata["status"] = "PREDICTED"
        metadata["trackNumber"] = record["properties"]["relative_orbit"]
        metadata["source"] = "bos-sarcat"
        metadata['processing_version'] = None

        folder_name = "acquisition-" + str(metadata["platform"]) + "_" \
                      + str(get_start_timestamp(metadata["sensingStart"])) + "_" \
                      + str(metadata["trackNumber"]) + "_" \
                      + str(metadata["sensoroperationalmode"]) \
                      + "-bos_sarcat-predicted"
        dataset_name = folder_name

        if os.path.isdir(folder_name):
            print("Folder already exists with name: {}".format(folder_name))
            print("Comparing the ingest times of existing vs new")
            existing = json.loads(open("{}/{}.met.json".format(folder_name, dataset_name, "r")).read())
            if is_outdated(existing_ingestion_time=existing["bos_ingestion_time"], new_ingestion_time=metadata["bos_ingestion_time"]):
                print("New dataset is more recent. Removing older dir.")
                try:
                    shutil.rmtree(folder_name)
                except Exception as ex:
                    print("Failed to delete dataset for {}. Because {}. {}".format(folder_name, ex.message,
                                                                                   traceback.format_exc()))
            else:
                print("Keeping existing dataset. Expect to see exception message.")

        try:
            print("Creating Dataset for {}".format(folder_name))
            os.mkdir(folder_name)
            met_file = open("%s/%s.met.json" % (folder_name, dataset_name), 'w')
            met_file.write(json.dumps(metadata))
            met_file.close()
            make_dataset_file(dataset_name, record, metadata["sensingStart"], metadata["sensingStop"])
        except Exception as ex:
            print("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))
            LOGGER.warn("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message,
                                                                                 traceback.format_exc()))


def create_product(record):
    product_name = record["properties"]["identifier"]
    status = record["properties"]["status"]
    start_time = record["properties"]["start_time"]
    end_time = record["properties"]["stop_time"]
    if not_RAW(product_name) and not past_planned(status, start_time, end_time):
        product_name = make_met_file(product_name, record)
        make_dataset_file(product_name, record)
        if "predicted" in record["properties"]:
            if record["properties"]["predicted"] is not None:
                print "Working with bos product : " + product_name
                make_predicted_datasets(record)


def main():
    try:
        bos_ingest_last = None
        from_time = None
        end_time = None
        get_cmd = "bos_sarcat_scrapper "


        context = open("_context.json", "r")
        ctx = json.loads(context.read())
        if ctx["bos_ingest_time"] != "" and ctx["bos_ingest_time"] is not None:
            bos_ingest_last = ctx["bos_ingest_time"]
        elif ctx["bos_ingest_time"] != "" and ctx["bos_ingest_time"] is not None:
            from_time = ctx["from_time"]
        else:
            # bos_ingest_last = get_redis_keys()
            if bos_ingest_last is None:
                raise ValueError("Key bos_ingest_last key not set in redis. Please set that first.")

        if ctx["end_time"] != "" and ctx["end_time"] is not None:
            end_time = ctx["end_time"]

        if bos_ingest_last is not None:
            get_cmd += "--fromBosIngestTime " + bos_ingest_last
        elif from_time is not None:
            get_cmd += '--fromTime ' + from_time

        if end_time is not None:
            get_cmd += "--toTime "+end_time

        get_cmd += " --sortBy bos_ingest --sort des"
        # print get_cmd
        p = subprocess.Popen(get_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = str(p.communicate())
        start_pos = output.find("{")
        end_pos = output.rfind("}")
        results = json.loads(output[start_pos:end_pos+1])
        # print results

        print("Number of acquisitions: %s" % results["totalFeatures"])
        if int(results["totalFeatures"]) != 0:
            most_recent = results["features"][0]["properties"]["bos_ingest"]
            print("Will update redis with ingest key: %s" % most_recent)
            for result in results["features"]:
                create_product(result)
            # update_redis_key(most_recent)
            ingest_acq.ingest_acq_dataset()
        else:
            LOGGER.info("No new SAR acquisitions since last scrape.")
            return
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    return

if __name__ == "__main__":
    main()
