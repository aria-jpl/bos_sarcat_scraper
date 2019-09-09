# BOS SARcat Scraper
## Purpose
#### This is a command line tool to get all acquisitions from BOS SARcat.
## Installation
1. Clone the git repository
> https://github.com/aria-jpl/bos_sarcat_scraper.git
2. Go into the project directory
> cd bos_sarcat_scraper
3. Install the bos_sarca_scrapper
> pip install -e .
### Requirements
Python dependencies:
- shapely
- geojson
- requests
##### These packages should be automatically installed if you don't already have them.
## Usage
You can use `bos_sarcat_scraper` on the command line to call the BOS SARcat API
```
$ bos_sarcat_scraper -h
usage: bos_sarcat_scraper [-h] [-from FROMTIME]
                           [--fromBosIngestTime FROMBOSINGESTTIME]
                           [-to TOTIME] [--spatialExtent SPATIALEXTENT]
                           [--sortBy SORTBY] [--sort SORT]

Query BOS SarCat for acquisitions.

optional arguments:
  -h, --help            show this help message and exit
  -from FROMTIME, --fromTime FROMTIME
                        specify the temporal start point in format , to get
                        acquisitions starting after the given timestamp in the
                        format yyyy-mm-ddThh:mm:ss.sss
  --fromBosIngestTime FROMBOSINGESTTIME
                        provide date and time in format , to get acquisitions
                        acquired by BOS after the given timestamp in the
                        format yyyy-mm-ddThh:mm:ss.sss
  -to TOTIME, --toTime TOTIME
                        specify the temporal end point in format , to get
                        acquisitions ending before the given timestamp in the
                        format yyyy-mm-ddThh:mm:ss.sss
  --spatialExtent SPATIALEXTENT
                        specify the area of interest in GeoJSON format
  --sortBy SORTBY       type "start_time" , "stop_time" or "bos_ingest" to
                        sort results by field
  --sort SORT           type "asc" or "des" to get results in ascending or
                        descending order of time respectively. If sortBy is
                        specified but sort is not, then defaults to ascending
```

