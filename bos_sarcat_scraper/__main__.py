from __future__ import absolute_import
from builtins import str
from builtins import input
import sys
import argparse
from . import bosart_scrape
import datetime
import json


def valid_date(s):
    try:
        try:
            date = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            date = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return date
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def geojson(spatial_extent):
    if type(json.loads(spatial_extent)) is dict:
        return spatial_extent


def sort_field(s_f):
    if s_f == "start_time" or s_f == "stop_time" or s_f == "bos_ingest":
        return s_f
    else:
        raise argparse.ArgumentError("The value for sortBy should be either start_time, stop_time or bos_ingest not %s."%s_f)


def sort_order(order):
    if order == "asc" or order == "des":
        return order
    else:
        raise argparse.ArgumentError("The value for sort should be either asc or des not %s,"%order)


def check_inputs(args):
    yes = "y"
    no = "n"
    if not args.fromTime and not args.fromBosIngestTime:
        print ("You have NOT specified any start time using --fromTime, -from or --fromBosIngestTime. \nYou are asking to find all acquisitions from the beginning of time! \nThis query will take a very long time.\nTHIS IS NOT RECOMMENDED.")
        resp = str(eval(input('Are you sure you want to proceed? (y/n):')))
        if resp.lower() == yes.lower():
            print("Okay! Please wait...")
            return True
        elif resp.lower() == no.lower():
            print("Please try again with the start time specified using --fromTime, -from or --fromBosIngestTime.")
            exit()
        else:
            print("Please specify y/n\n")
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description='Query BOS SarCat for acquisitions.')
    parser.add_argument("-from","--fromTime", help='specify the temporal start point in format , to get acquisitions starting after the given timestamp in the format yyyy-mm-ddThh:mm:ss.sssZ', type=valid_date)
    parser.add_argument("--fromBosIngestTime", help='provide date and time in format , to get acquisitions acquired by BOS after the given timestamp in the format yyyy-mm-ddThh:mm:ss.sssZ', type=valid_date)
    parser.add_argument("-to","--toTime", help='specify the temporal end point in format , to get acquisitions ending before the given timestamp in the format yyyy-mm-ddThh:mm:ss.sssZ', type=valid_date)
    parser.add_argument("--spatialExtent", help='specify the area of interest in GeoJSON format', type = geojson)
    parser.add_argument("--sortBy", help='type "start_time" , "stop_time" or "bos_ingest" to sort results by field', type = sort_field)
    parser.add_argument("--sort", help='type "asc" or "des" to get results in ascending or descending order of time respectively. If sortBy is specified but sort is not, then defaults to ascending', type = sort_order)
    args = parser.parse_args()

    checked = False

    while not checked:
        checked = check_inputs(args)

    # construct the parameter list based on user specified restrictions
    params = {}
    if args.fromTime:
        params["fromTime"] = args.fromTime
    if args.fromBosIngestTime:
        params["fromBosIngestTime"] = args.fromBosIngestTime
    if args.toTime:
        params["toTime"] = args.toTime
    if args.spatialExtent:
        params["spatialExtent"] = json.dumps(args.spatialExtent)
    if args.sortBy:
        params["sortBy"] = args.sortBy
    if args.sort:
        params["sort"] = args.sort

    print(bosart_scrape.make_api_call(parameters=params))


if __name__ == '__main__':
    main()
