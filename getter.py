# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import logging
import argparse
import json
import datetime
import requests
import sys

from influxdb import InfluxDBClient

# --------------------------------------------------------------------------- #
# Functions
# --------------------------------------------------------------------------- #


def check_value(meas_value, gain, offset, wrong_value, def_value):
    if meas_value == wrong_value:
        return def_value
    else:
        return meas_value * gain + offset

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='configuration file')
    arg_parser.add_argument('-l', help='log file')

    args = arg_parser.parse_args()
    config = json.loads(open(args.c).read())

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    if not args.l:
        log_file = None
    else:
        log_file = args.l

    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)
    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    ts = datetime.datetime.utcnow()
    str_out_file_ts_desc = ts.strftime('%Y%m%d%H%M%S')

    influxdb_data_points = []
    for cnt_location in range(0, len(config['location_parameters'])):
        r_get = None
        cfg_single_location = config['location_parameters'][cnt_location]

        logger.info('Retrieving data for location \'%s\'', cfg_single_location['output_csv_file_header'])
        headers = {'soda-user': config['location_parameters'][cnt_location]['user'],
                   'soda-passwd': config['location_parameters'][cnt_location]['password']}

        logger.info('Retrieving data from %s', cfg_single_location['url1'])
        get_url = "%s?geopoint=%s,%s&elevation=%s&firstday=%s&lastday=%s&duration=%s&time=%s&slope=%s&azimuth=%s&albedo=%s&horizon=%s" % \
                  (cfg_single_location['url1'], cfg_single_location['latitude'], cfg_single_location['longitude'],
                   cfg_single_location['altitude'], tomorrow.isoformat(), tomorrow.isoformat(),
                   cfg_single_location['duration'], cfg_single_location['time'], cfg_single_location['slope'],
                   cfg_single_location['azimuth'], cfg_single_location['albedo'], cfg_single_location['horizon'])
        r_url1 = requests.get(get_url, headers=headers)

        if r_url1.status_code != 200 or (r_url1.status_code == 200 and "ERROR" in r_url1.text):
            logger.error('Unable to retrieve data from %s', cfg_single_location['url1'])
            logger.info('Retrieving data from %s', cfg_single_location['url2'])
            get_url = "%s?geopoint=%s,%s&elevation=%s&firstday=%s&lastday=%s&duration=%s&time=%s&slope=%s&azimuth=%s&albedo=%s&horizon=%s" % \
                      (cfg_single_location['url2'], cfg_single_location['latitude'], cfg_single_location['longitude'],
                       cfg_single_location['altitude'], tomorrow.isoformat(), tomorrow.isoformat(),
                       cfg_single_location['duration'], cfg_single_location['time'], cfg_single_location['slope'],
                       cfg_single_location['azimuth'], cfg_single_location['albedo'], cfg_single_location['horizon'])
            r_url2 = requests.get(get_url, headers=headers)
            if r_url2.status_code != 200 or (r_url2.status_code == 200 and "ERROR" in r_url2.text):
                logger.error('Unable to retrieve data from %s', cfg_single_location['url2'])
            else:
                r_get = r_url2
        else:
            r_get = r_url1

        if r_get is not None:
            output_file_path = '%s/%s_%sUTC.csv' % (cfg_single_location['output_folder'],
                                                    cfg_single_location['output_csv_file_header'],
                                                    str_out_file_ts_desc)
            logger.info('Saving data in %s', output_file_path)
            output_file = open(output_file_path, 'w')
            output_file.write(r_get.text)
            output_file.close()

        if cfg_single_location['influxdb_connection']['data_inserting'] == 'enabled':
            time_precision = cfg_single_location['influxdb_connection']['time_precision']
            output_file = open(output_file_path, 'r')
            str_data = output_file.read()
            output_file.close()
            lines = str_data.split("\n")

            # Read the file content
            for line in lines:
                if '#' not in line and len(line) > 0:
                    (y, m, d, h, dir_i, dif_i, ref, glob_i, dir_h, dif_h, glob_h, code, toa, stuff) = line.split(' ')

                    # Check values and transform Wh/m^2 in W/m^2 (if needed)
                    gain = 4
                    step = int(float(h) * gain)
                    dir_i = check_value(meas_value=float(dir_i), gain=gain, offset=0,
                                        wrong_value=-999, def_value=-999)
                    dif_i = check_value(meas_value=float(dif_i), gain=gain, offset=0,
                                        wrong_value=-999, def_value=-999)
                    ref = check_value(meas_value=float(ref), gain=gain, offset=0,
                                      wrong_value=-999, def_value=-999)
                    glob_i = check_value(meas_value=float(glob_i), gain=gain, offset=0,
                                         wrong_value=-999, def_value=-999)
                    dir_h = check_value(meas_value=float(dir_h), gain=gain, offset=0,
                                        wrong_value=-999, def_value=-999)
                    dif_h = check_value(meas_value=float(dif_h), gain=gain, offset=0,
                                        wrong_value=-999, def_value=-999)
                    glob_h = check_value(meas_value=float(glob_h), gain=gain, offset=0,
                                         wrong_value=-999, def_value=-999)
                    code = check_value(meas_value=int(code), gain=1, offset=0,
                                       wrong_value=-999, def_value=-999)
                    toa = check_value(meas_value=float(toa), gain=gain, offset=0,
                                      wrong_value=-999, def_value=-999)

                    point = {
                                'time': ts,
                                'measurement': cfg_single_location['influxdb_connection']['measurement'],
                                'fields': dict(DirInclG=float(dir_i), DifInclG=float(dif_i), RefG=float(ref),
                                               GlobInclG=float(glob_i), DirHorG=float(dir_h), DifHorG=float(dif_h),
                                               GlobHorG=float(dir_h), Code=int(code), TOA=float(toa), Step=int(step)),
                                'tags': dict(location=cfg_single_location['output_csv_file_header'],
                                             latitude=cfg_single_location['latitude'],
                                             longitude=cfg_single_location['longitude'],
                                             altitude=cfg_single_location['altitude'],
                                             soda_db=cfg_single_location['soda_db'],
                                             step_tag=step)
                            }
                    influxdb_data_points.append(point)

    # --------------------------------------------------------------------------- #
    # InfluxDB connection
    # --------------------------------------------------------------------------- #
    logger.info("Connection to InfluxDB server on [%s:%s]" % (cfg_single_location['influxdb_connection']['host'],
                                                              cfg_single_location['influxdb_connection']['port']))
    try:
        idb_client = InfluxDBClient(host=cfg_single_location['influxdb_connection']['host'],
                                    port=int(cfg_single_location['influxdb_connection']['port']),
                                    username=cfg_single_location['influxdb_connection']['user'],
                                    password=cfg_single_location['influxdb_connection']['password'],
                                    database=cfg_single_location['influxdb_connection']['db'])
    except Exception as e:
        logger.error("EXCEPTION: %s" % str(e))
        sys.exit(2)
    logger.info("Connection successful")

    if len(influxdb_data_points) > 0:
        try:
            res = idb_client.write_points(influxdb_data_points, time_precision=time_precision)
        except Exception as e:
            logger.error("EXCEPTION: %s" % str(e))
            sys.exit(3)

        if res is True:
            logger.info('Inserted correctly %i samples' % len(influxdb_data_points))
        else:
            logger.info('Unable to insert data')
    else:
        logger.info('No data to insert')

    logger.info("Ending program")
