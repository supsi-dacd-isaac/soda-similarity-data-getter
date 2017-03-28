# soda-similarity-data-getter
Script to get data from Similarity forecast service of SoDa (http://www.soda-pro.com/soda-products/hc3-similarity-forecast)

It gathers the forecasts from several locations and save data in csv files (one for location). Optionally, the forecasts can be stored in an InfluxDB database (https://www.influxdata.com/). 

Tested only with Python3.

**Requirements:**
* Python>=3.5.2
* json>=2.6.0
* requests>=2.13.0
* influxdb>=4.0.0


**Usage:**
<pre>
/usr/bin/python getter.py -c conf/example.json
</pre>
