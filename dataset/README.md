## ES Setup for SAR Availability Tool
1. Update the datasets template on mozart with the contents of dataset.json file
2. Add an alias `acquisition` to the ES index containing the bos acquisitions. This needs to be done as the SAR Availability facet veiw queries over this alias.
```
# ssh onto GRQ
$ ssh -i [path to PEM file] ops@[GRQ_IP]

# add alias to bos index
$ curl -XPOST 'http://localhost:9200/_aliases' -d '
  {
     "actions" : [
         { "add" : { "index" : "grq_v2.0_acquisition-bos_sarcat", "alias" : "acquisition" } }
     ]
  }'

# add alias to s1-iw-slc index
$ curl -XPOST 'http://localhost:9200/_aliases' -d '
  {
     "actions" : [
         { "add" : { "index" : "grq_v2.0_acquisition-s1-iw_slc", "alias" : "acquisition" } }
     ]
  }'
```