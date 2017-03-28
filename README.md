# soda-similarity-data-getter
Script to get data from Similarity forecast service of SoDa (http://www.soda-pro.com/soda-products/hc3-similarity-forecast)

It saves data in csv file or (optional) on an InfluxDB database (https://www.influxdata.com/). 

**Requirements:**
* Python>=3.5.2
* json>=2.6.0
* requests>=2.13.0
* influxdb>=4.0.0


**Usage:**
<pre>
/usr/bin/python getter.py -c conf/supsi.json
</pre>