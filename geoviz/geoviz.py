#!/usr/bin/env python

import gflags
import httplib2
import logging
import math
import numpy
import pprint
import sys

from apiclient.discovery import build
from apiclient.errors import HttpError

#from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run

from scipy.misc import pilutil

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('start_year', 2009, 'Start processing from this year')
gflags.DEFINE_integer('start_month', 1, 'Start processing from this month')
gflags.RegisterValidator('start_month',
                         lambda value: value >= 1 and value <= 12,
                         message='Invalid start month')
gflags.DEFINE_integer('end_year', 2010, 'Start processing from this year')
gflags.DEFINE_integer('end_month', 1, 'Start processing from this month')
gflags.RegisterValidator('end_month',
                         lambda value: value >= 1 and value <= 12,
                         message='Invalid end month')

gflags.DEFINE_integer('width', 1920, 'Width of output')
gflags.DEFINE_integer('height', 1440, 'Height of output')

gflags.DEFINE_string('output', '', 'Output prefix', short_name = 'o')
gflags.DEFINE_string('priority', 'BATCH', 'Priority at which to run the query',
                     short_name = 'p')
gflags.RegisterValidator(
    'priority', lambda value: value == 'BATCH' or value == 'INTERACTIVE',
    message='--priority must be \'BATCH\' or \'INTERACTIVE\'')

gflags.DEFINE_string('color_field', 'minrtt',
    'Field to use to color the pixels', short_name = 'c')
gflags.RegisterValidator(
    'color_field', lambda value: value == 'minrtt' or value == 'packetloss',
    message='--color_field must be \'minrtt\' or \'packetloss\'')

logging.basicConfig(format = '[%(asctime)s] %(levelname)s: %(message)s',
                    level = logging.DEBUG)

PROJECT_ID = 'measurement-lab'
DATASET_ID = 'm_lab'
FLOW = flow_from_clientsecrets('client_secrets.json',
                               scope='https://www.googleapis.com/auth/bigquery')

# The MinRTT pixel is coloured based on the following constants. MinRTT outside
# of the range described by these constants are ignored.
MINRTT_BLUE = 0
MINRTT_GREEN = 80
MINRTT_RED = 1000

# The Packet Loss pixel is coloured based on the following constants. Packet
# Loss outside of the range described is ignored.
PACKETLOSS_GREEN = 0
PACKETLOSS_RED = 1.0

# Set the GAMMA to a higher number to boost the color of high packet loss.
PACKETLOSS_GAMMA = 4.0

BQ_SERVICE = None
MAP_ARRAY = None
COLOR_FUNC = None


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y


def linear_projection(latitude, longitude):
    return Point(longitude, latitude)

def cylindrical_equidistant_projection(latitude, longitude):
    # If longitude0 is non-zero, don't forget to wrap x.
    longitude0 = 0.0
    phi = 0.0  # equirectangular
    # phi = 0.654498469  # Miller 1
    # phi = 0.750491578  # Miller 2
    # phi = 0.880809496  # Miller 3
    return Point((longitude - longitude0) * math.cos(phi), latitude)

def mercator_projection(latitude, longitude):
    # If longitude0 is non-zero, don't forget to wrap x.
    longitude0 = 0.0
    latitude_rad = math.radians(latitude)
    return Point(
        longitude - longitude0,
        math.degrees(math.log(math.tan(math.pi / 4 + latitude_rad / 2)))
    )

# TODO(dominic): Choice of projection from flags
#PROJECTION_FUNC = linear_projection
PROJECTION_FUNC = cylindrical_equidistant_projection
#PROJECTION_FUNC = mercator_projection

def get_color_for_minrtt(minrtt):
    if minrtt < MINRTT_BLUE or minrtt > MINRTT_RED:
        return [0, 0, 0]
    elif minrtt > MINRTT_GREEN:
        col = (minrtt - MINRTT_GREEN) / (MINRTT_RED - MINRTT_GREEN)
        return [col, 1.0 - col, 0]
    else:
        col = (minrtt - MINRTT_BLUE) / (MINRTT_GREEN - MINRTT_BLUE)
        return [0, col, 1.0 - col]

def get_color_for_packetloss(packet_loss):
    if packet_loss < PACKETLOSS_GREEN or packet_loss > PACKETLOSS_RED:
        logging.warning('rejecting %.3f', packet_loss)
        return [0, 0, 0]
    else:
        col = (packet_loss - PACKETLOSS_GREEN) / \
              (PACKETLOSS_RED - PACKETLOSS_GREEN)
        col = math.pow(col, 1.0 / PACKETLOSS_GAMMA)
        return [col, 1.0 - col, 0.0]

COLOR_FIELDS = {
    'minrtt': {
        'func': get_color_for_minrtt,
        'select': 'web100_log_entry.snap.MinRTT',
        'where': 'web100_log_entry.snap.MinRTT < %s' % MINRTT_RED},
    'packetloss': {
        'func': get_color_for_packetloss,
        'select': 'web100_log_entry.snap.SegsRetrans / ' \
                  'web100_log_entry.snap.DataSegsOut',
        'where': 'web100_log_entry.snap.DataSegsOut > 0'}
}


def authorize_and_build_bigquery_service():
    storage = Storage('bigquery_credentials.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run(FLOW, storage)

    http = httplib2.Http()

    logging.info('Authorizing...')
    http = credentials.authorize(http)

    logging.info('Building BQ service...')
    return build('bigquery', 'v2', http = http)

def plot_rows(rows):
    for item in rows:
        item = item['f']
        color_key = float(item[0]['v'])
        latitude = float(item[1]['v'])
        longitude = float(item[2]['v'])

        if longitude < -180 or longitude > 180:
            logging.error('Invalid longitude %.3f', longitude)
        if latitude < -90 or latitude > 90:
            logging.error('Invalid latitude %.3f', latitude)

        projected = PROJECTION_FUNC(latitude, longitude)
        if projected.x < -180 or projected.x > 180:
            logging.warn('Invalid projected longitude %.3f', projected.x)
        if projected.y < -90 or projected.y > 90:
            logging.warn('Invalid projected latitude %.3f', projected.y)

        map_coord = Point(FLAGS.width * (projected.x / 360.0 + 0.5),
                          FLAGS.height * (1.0 - (projected.y / 180.0 + 0.5)))

        try:
            color = COLOR_FUNC(color_key)
            # These coordinates are not reversed - rows first
            MAP_ARRAY[map_coord.y, map_coord.x] += color

        except IndexError:
            logging.error('Bad map coord: %s', pprint.pprint(map_coord))

def get_tables():
    try:
        tables = BQ_SERVICE.tables()
        reply = tables.list(projectId = PROJECT_ID,
                            datasetId = DATASET_ID).execute()
        return reply['tables']

    except HttpError as err:
        logging.error('Failed to list tables: %s', pprint.pprint(err.content))
        sys.exit(1)

def run_query_and_save_map(table):
    global MAP_ARRAY
    global COLOR_FUNC

    # These dimensions are not reversed - number of rows is first
    MAP_ARRAY = numpy.zeros((FLAGS.height, FLAGS.width, 3), dtype = numpy.float)

    # Set color_field from FLAGS
    color_field = COLOR_FIELDS[FLAGS.color_field]
    COLOR_FUNC = color_field['func']

    try:
        query = ("SELECT %s,"
                 "connection_spec.client_geolocation.latitude,"
                 "connection_spec.client_geolocation.longitude "
                 "FROM [%s.%s] "
                 "WHERE log_time > 0 AND %s AND "
                 "web100_log_entry.is_last_entry == true") % \
                (color_field['select'], DATASET_ID, table, color_field['where'])
        logging.info('Running %s', query)
        job_collection = BQ_SERVICE.jobs()
        job_data = {
            'configuration': {
                'query': {
                    'query': query,
                    'priority': FLAGS.priority
                }
            }
        }

        insert_response = job_collection.insert(projectId = PROJECT_ID,
                                              body = job_data).execute()

        import time
        current_status = 'INVALID'
        while current_status != 'DONE':
            time.sleep(30)
            status = job_collection.get(
                projectId = PROJECT_ID,
                jobId = insert_response['jobReference']['jobId']).execute()
            current_status = status['status']['state']
            logging.info('%s', current_status)

        current_row = 0
        logging.info('getQueryResults %d', current_row)
        query_reply = job_collection.getQueryResults(
            projectId = PROJECT_ID,
            jobId = insert_response['jobReference']['jobId'],
            startIndex = current_row).execute()

        total_rows = int(query_reply['totalRows'])
        while ('rows' in query_reply) and current_row < total_rows:
            logging.info('Received rows from %d / %d [%.2f%%]',
                         current_row,
                         total_rows,
                         100.0 * float(current_row) / float(total_rows))
            plot_rows(query_reply['rows'])
            current_row += len(query_reply['rows'])
            logging.info('getQueryResults %d', current_row)
            query_reply = job_collection.getQueryResults(
                projectId = PROJECT_ID,
                jobId = query_reply['jobReference']['jobId'],
                startIndex = current_row).execute()

        # convert to image and show
        # img = pilutil.toimage(MAP_ARRAY)
        # img.show()

        # TODO(dominic): Normalize/gamma correct on flag
        MAP_ARRAY.clip(0.0, 1.0, out=MAP_ARRAY)
        
        # save image to disk
        output_name = FLAGS.output + table + '.bmp'
        logging.info('Saving map to %s', output_name)
        pilutil.imsave(output_name, MAP_ARRAY)

    except HttpError as err:
        logging.error('Error running query: %s', pprint.pprint(err.content))
        sys.exit(1)

    except AccessTokenRefreshError:
        logging.error('Credentials have been revoked or expired. Please re-run '
                      'the application to re-authorize.')
        sys.exit(1)

def main():
    global BQ_SERVICE

    try:
        FLAGS(sys.argv)
    except gflags.FlagsError, err:
        print '%s\nUsage: %s ARGS\n%s' % (err, sys.argv[0], FLAGS)
        sys.exit(1)

    BQ_SERVICE = authorize_and_build_bigquery_service()
  
    tables = get_tables()
  
    for table in tables:
        table_id = table['tableReference']['tableId']
        if not "_" in table_id:
            continue
        year, month = (int(x) for x in table_id.split('_'))
        if (year > FLAGS.start_year or \
            (year == FLAGS.start_year and month >= FLAGS.start_month)) and \
           (year < FLAGS.end_year or \
            (year == FLAGS.end_year and month <= FLAGS.end_month)):
            logging.info('Running query for %s', table_id)
            run_query_and_save_map(table_id)
    logging.info('Complete')


if __name__ == '__main__':
    main()

