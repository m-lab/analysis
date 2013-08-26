#!/usr/bin/env python

import gflags
import json
import logging
import math
import numpy
import os
import pprint
import sys

from scipy.misc import pilutil

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('start_year', 2013, 'Start processing from this year')
gflags.DEFINE_integer('start_month', 1, 'Start processing from this month')
gflags.RegisterValidator('start_month',
                         lambda value: value >= 1 and value <= 12,
                         message='Invalid start month')
gflags.DEFINE_integer('end_year', 2013, 'End processing with this year')
gflags.DEFINE_integer('end_month', 2, 'End processing with this month')
gflags.RegisterValidator('end_month',
                         lambda value: value >= 1 and value <= 12,
                         message='Invalid end month')

gflags.DEFINE_integer('width', 1920, 'Width of output')
gflags.DEFINE_integer('height', 1440, 'Height of output')

gflags.DEFINE_string('output', '', 'Output prefix', short_name = 'o')

gflags.DEFINE_string('test_type', 'ping', 'Test type to visualize.',
    short_name = 't')

gflags.DEFINE_string('color_field', 'mean_rtt_ms',
    'Field to use to color the pixels', short_name = 'c')
# TODO: validate color_field based on test_type
gflags.RegisterValidator(
    'color_field',
    lambda value: value == 'mean_rtt_ms' or value == 'max_rtt_ms' or value == 'packet_loss',
    message='--color_field passed an invalid value')

logging.basicConfig(format = '[%(asctime)s] %(levelname)s: %(message)s',
                    level = logging.INFO)

# The MinRTT pixel is coloured based on the following constants. MinRTT outside
# of the range described by these constants are ignored.
RTT_BLUE = 0
RTT_GREEN = 80
RTT_RED = 1000

# The Packet Loss pixel is coloured based on the following constants. Packet
# Loss outside of the range described is ignored.
PACKETLOSS_GREEN = 0
PACKETLOSS_RED = 1.0

# Set the GAMMA to a higher number to boost the color of high packet loss.
PACKETLOSS_GAMMA = 4.0

MAP_ARRAY = None
COLOR_FUNC = None
COLOR_VALUE = None


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


def get_color_for_rtt(rtt):
    if rtt < RTT_BLUE or rtt > RTT_RED:
        return [0, 0, 0]
    elif rtt > RTT_GREEN:
        col = (rtt - RTT_GREEN) / (RTT_RED - RTT_GREEN)
        return [col, 1.0 - col, 0]
    else:
        col = (rtt - RTT_BLUE) / (RTT_GREEN - RTT_BLUE)
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
    'mean_rtt_ms': get_color_for_rtt,
    'max_rtt_ms': get_color_for_rtt,
}


def plot_item(item):
    global MAP_ARRAY

    logging.debug('Converting %s', item)
    color_key = None
    for v in item["values"]:
        logging.debug('  %s', v)
        if v["name"] == FLAGS.color_field:
            color_key = v["value"]
            logging.debug('    %s', color_key)
            break
    if color_key == None:
        # logging.warning('No values found for item %s',
        #     item["device_properties"]["timestamp"])
        return
    location = item["device_properties"]["location"]
    latitude = location["latitude"]
    longitude = location["longitude"]

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
        color = COLOR_FUNC(float(color_key))
        # These coordinates are not reversed - rows first
        logging.debug("setting %d.%d to %.2f %.2f %.2f", map_coord.y, map_coord.x,
            color[0], color[1], color[2])
        MAP_ARRAY[map_coord.y, map_coord.x] += color

    except IndexError:
        logging.error('Bad map coord: %s', pprint.pprint(map_coord))


def main():
    global COLOR_FUNC
    global MAP_ARRAY

    try:
        FLAGS(sys.argv)
    except gflags.FlagsError, err:
        print '%s\nUsage: %s ARGS\n%s' % (err, sys.argv[0], FLAGS)
        sys.exit(1)

    # Set color_field from FLAGS
    COLOR_FUNC = COLOR_FIELDS[FLAGS.color_field]

    try:
        year = FLAGS.start_year
        month = FLAGS.start_month
        while year < FLAGS.end_year or (year == FLAGS.end_year and month <= FLAGS.end_month):
            # These dimensions are not reversed - number of rows is first
            MAP_ARRAY = numpy.zeros((FLAGS.height, FLAGS.width, 3), dtype = numpy.float)
            logging.info('Running query for %d.%d', year, month)
            # Open every json file in folder data/year/month/*.json
            file_contents = ""
            directory = os.path.join("data",str(year).zfill(2),str(month).zfill(2))
            logging.info("Checking %s", directory)
            for root,dirs,files in os.walk(directory):
                for file in files:
                    if file.endswith(".json"):
                        logging.info("  Opening %s", file)
                        f = open(os.path.join(directory, file), 'r')
                        for line in f:
                            if line == None:
                                break
                            item = json.JSONDecoder().decode(line)
                            # TODO: filter item against color_field.select and type
                            if item["type"] == FLAGS.test_type:
                                plot_item(item)
                        f.close()
            # convert to image and show
            # img = pilutil.toimage(MAP_ARRAY)
            # img.show()

            # TODO(dominic): Normalize/gamma correct on flag
            MAP_ARRAY.clip(0.0, 1.0, out=MAP_ARRAY)
            
            # save image to disk
            output_name = FLAGS.output + FLAGS.test_type + '.' + \
                FLAGS.color_field + '.' + str(year) + '.' + str(month).zfill(2) + '.bmp'
            logging.info('Saving map to %s', output_name)
            pilutil.imsave(output_name, MAP_ARRAY)
           
            month += 1
            if month > 12:
                month -= 12
                year += 1

    except Exception as e:
        logging.error(e)
        sys.exit(1)

    logging.info('Complete')


if __name__ == '__main__':
    main()

