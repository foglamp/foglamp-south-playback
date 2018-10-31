# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Module for playback async plugin """

import asyncio
import copy
import csv
import uuid
import os
import json
import logging
import datetime
import time
from threading import Event
from dateutil import parser

from queue import Queue
from threading import Thread, Condition

from foglamp.common import logger
from foglamp.plugins.common import utils
from foglamp.services.south import exceptions
from foglamp.services.south.ingest import Ingest


__author__ = "Amarendra Kumar Sinha"
__copyright__ = "Copyright (c) 2018 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Test Sample',
        'type': 'string',
        'default': 'playback',
        'readonly': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'sample',
        'displayName': 'Asset Name',
        'order': '1'
    },
    'csvFilename': {
        'description': 'CSV File name',
        'type': 'string',
        'default': 'sinusoid.csv',
        'displayName': 'CSV file name with extension',
        'order': '2'
    },
    'headerRow': {
        'description': 'If header row is preset as first row of CSV file',
        'type': 'boolean',
        'default': 'true',
        'displayName': 'Header Row?',
        'order': '3'
    },
    'fieldNames': {
        'description': 'Comma separated list of column names, if headerRow is false',
        'type': 'string',
        'default': 'None',
        'displayName': 'Header columns',
        'order': '4'
    },
    'readingCols': {
        'description': 'Cherry pick data columns with the same/new name e.g. {"readings": "readings", "ts": "timestamp"}',
        'type': 'JSON',
        'default': '{}',
        'displayName': 'Cherry pick column with same/new name',
        'order': '5'
    },
    'timestampFromFile': {
        'description': 'Time Delta to be choosen from a column in the CSV',
        'type': 'boolean',
        'default': 'false',
        'displayName': 'Time stamp from file?',
        'order': '6'
    },
    'timestampCol': {
        'description': 'Timestamp header column',
        'type': 'string',
        'default': 'ts',
        'displayName': 'Timestamp column name',
        'order': '7'
    },
    'timestampFormat': {
        'description': 'Timestamp format in File',
        'type': 'string',
        'default': '%Y-%m-%d %H:%M:%S.%f',
        'displayName': 'Time stamp format',
        'order': '8'
    },
    'ingestMode': {
        'description': 'Mode of data ingest - burst/realtime/batch',
        'type': 'enumeration',
        'default': 'burst',
        'options': ['burst', 'realtime', 'batch'],
        'displayName': 'Ingest mode?',
        'order': '9'
    },
    'sampleRate': {
        'description': 'No. of readings per sec',
        'type': 'integer',
        'default': '100',
        'displayName': 'No. of samples per sec',
        'minimum': '1',
        'maximum': '1000000',
        'order': '10'
    },
    'burstInterval': {
        'description': 'Time interval between consecutive bursts in milliseconds',
        'type': 'integer',
        'default': '1000',
        'displayName': 'Burst Interval (ms)',
        'minimum': '1',
        'order': '11'
    },
    'burstSize': {
        'description': 'No. of data points in one burst',
        'type': 'integer',
        'default': '1',
        'displayName': 'Data points per burst',
        'minimum': '1',
        'order': '12'
    },
    'repeatLoop': {
        'description': 'Read CSV File in an endless loop',
        'type': 'boolean',
        'default': 'false',
        'displayName': 'Read from file in a loop?',
        'order': '13'
    },
    'bucketSize': {
        'description': 'No. of items in the shared queue between Producer and Consumer. '
                       'As a rule of thumb, burst mode bucket size should be much smaller (around 10) than '
                       'realtime/batch mode bucket size (around 1000)',
        'type': 'integer',
        'default': '1000',
        'minimum': '10',
        'displayName': 'Queue size',
        'order': '14'
    },
}

_FOGLAMP_ROOT = os.getenv("FOGLAMP_ROOT", default='/usr/local/foglamp')
_FOGLAMP_DATA = os.path.expanduser(_FOGLAMP_ROOT + '/data')
_LOGGER = logger.setup(__name__, level=logging.INFO)
producer = None
consumer = None
bucket = None
condition = None
BUCKET_SIZE = 10
wait_event = Event()
wait_event.clear()


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """
    return {
        'name': 'Playback',
        'version': '1.0',
        'mode': 'async',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """ Initialise the plugin.
    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        data: JSON object to be used in future calls to the plugin
    Raises:
    """
    data = copy.deepcopy(config)
    try:
        if not data['csvFilename']['value']:
            raise RuntimeError("csv filename cannot be empty")
        if int(data['sampleRate']['value']) < 1 or int(data['sampleRate']['value']) > 1000000:
            raise RuntimeError("sampleRate should be in range 1-1000000")
        if int(data['burstSize']['value']) < 1:
            raise RuntimeError("burstSize should not be less than 1")
        if int(data['burstInterval']['value']) < 1:
            raise RuntimeError("burstInterval should not be less than 1")
        if data['ingestMode']['value'] not in ['burst', 'realtime', 'batch']:
            raise RuntimeError("ingestMode should be one of ('burst', 'realtime', 'batch')")
        if int(data['bucketSize']['value']) < 10:
            raise RuntimeError("bucketSize should not be less than 10")
    except KeyError:
        raise
    except RuntimeError:
        raise
    return data


def plugin_start(handle):
    """ Extracts data from the playback and returns it in a JSON document as a Python dict.
    Available for async mode only.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        a playback reading in a JSON document, as a Python dict
    """
    global producer, consumer, bucket, condition

    loop = asyncio.get_event_loop()
    condition = Condition()
    BUCKET_SIZE = int(handle['bucketSize']['value'])
    bucket = Queue(BUCKET_SIZE)
    producer = Producer(bucket, condition, handle, loop)
    consumer = Consumer(bucket, condition, handle, loop)

    producer.start()
    consumer.start()
    bucket.join()


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    _LOGGER.info("Old config for playback plugin {} \n new config {}".format(handle, new_config))
    # Find diff between old config and new config
    diff = utils.get_diff(handle, new_config)
    # Plugin should re-initialize and restart if key configuration is changed
    if 'assetName' in diff or 'csvFilename' in diff or 'headerRow' in diff or \
        'fieldNames' in diff or 'readingCols' in diff or 'timestampFromFile' in diff or \
        'timestampCol' in diff or 'timestampFormat' in diff or \
        'sampleRate' in diff or 'ingestMode' in diff or 'burstSize' in diff or 'burstInterval' in diff or \
        'repeatLoop' in diff or 'bucketSize' in diff:
        plugin_shutdown(handle)
        new_handle = plugin_init(new_config)
        new_handle['restart'] = 'yes'
        _LOGGER.info("Restarting playback plugin due to change in configuration key [{}]".format(', '.join(diff)))
    else:
        new_handle = copy.deepcopy(new_config)
        new_handle['restart'] = 'no'
    return new_handle


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South plugin service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        plugin shutdown
    """
    global producer, consumer

    if producer is not None:
        producer._tstate_lock = None
        producer._stop()
        producer = None
    if consumer is not None:
        consumer._tstate_lock = None
        consumer._stop()
        consumer = None
    _LOGGER.info('playback plugin shut down.')


class Producer(Thread):
    def __init__(self, queue, condition, handle, loop):
        super(Producer, self).__init__()
        self.queue = queue
        self.condition = condition
        self.handle = handle
        self.loop = loop

        try:
            if self.handle['ingestMode']['value'] == 'burst':
                burst_interval = int(self.handle['burstInterval']['value'])
                self.period = round(burst_interval / 1000.0, len(str(burst_interval)) + 1)
            else:
                recs = int(self.handle['sampleRate']['value'])
                self.period = round(1.0 / recs, len(str(recs)) + 1)
        except ZeroDivisionError:
            _LOGGER.warning('sampleRate must be greater than 0, defaulting to 1')
            self.period = 1.0

        self.csv_file_name = "{}/{}".format(_FOGLAMP_DATA, self.handle['csvFilename']['value'])
        self.iter_sensor_data = iter(self.get_data())

        # Cherry pick columns from readings and if desired, with
        self.reading_cols = json.loads(self.handle['readingCols']['value'])

        self.prv_readings_ts = None
        self.timestamp_interval = None

    def get_data(self):
        # TODO: Improve this?
        # This will handle timestamp for first row only as there is no prv row to compare timestamp with
        if self.handle['timestampFromFile']['value'] == 'true':
            ts_col = self.handle['timestampCol']['value']
            if ts_col != 'None':
                with open(self.csv_file_name, 'r' ) as f:
                    field_names = self.handle['fieldNames']['value'].split(",") if self.handle['headerRow']['value'] == 'false' else None
                    reader = csv.DictReader(f, fieldnames=field_names)
                    line = next(reader)
                    first_ts = parser.parse(line[ts_col])
                    line = next(reader)
                    second_ts = parser.parse(line[ts_col])
                    self.timestamp_interval = (second_ts - first_ts).total_seconds()

        with open(self.csv_file_name, 'r' ) as data_file:
            # TODO: Audo detect header
            # headr = data_file.readline()
            # has_header = csv.Sniffer().has_header(headr)
            # data_file.seek(0)  # Rewind.
            # col_labels = None if has_header else field_names
            # reader = csv.DictReader(data_file, fieldnames=col_labels)
            field_names = self.handle['fieldNames']['value'].split(",") if self.handle['headerRow']['value'] == 'false' else None
            reader = csv.DictReader(data_file, fieldnames=field_names)
            for line in reader:
                yield line

    def get_time_stamp_diff(self, readings):
        # The option to have the timestamp come from a column in the CSV file. The first timestamp should
        # be treated as a base time for all the readings and the current time substituted for that time stamp.
        # Successive readings should be sent with the same time delta as the times in the file. I.e. if the
        # file contains a timestamp series 14:01:23, 14:01:53, 14:02:23,.. and the time we sent the first
        # row of data is 18:15:45 then the second row should be sent at 18:16:15, preserving the same time
        # difference between the rows.
        ts_col = self.handle['timestampCol']['value']
        ts_format = self.handle['timestampFormat']['value']
        c = 0
        if ts_col != 'None':
            if ts_format == 'None':
                readings_ts = parser.parse(readings[ts_col])
            else:
                readings_ts = datetime.datetime.strptime(readings[ts_col], ts_format)
            if self.prv_readings_ts is None:
                c = self.timestamp_interval  # For first row only
            else:
                c = readings_ts - self.prv_readings_ts
                c = c.total_seconds()
            self.prv_readings_ts = readings_ts
        return c

    def run(self):
        while True:
            time_start = time.time()
            time_stamp = None
            sensor_data = {}
            data_count = 0
            try:
                time_stamp = utils.local_timestamp()
                if self.handle['ingestMode']['value'] == 'burst':
                    # Support for burst of data. Allow a burst size to be defined in the configuration, default 1.
                    # If a size of greater than 1 is set then that number of input values should be sent as an
                    # array of value. E.g. with a burst size of 10, which data point in the reading will be an
                    # array of 10 elements.
                    burst_data_points = []
                    for i in range(int(self.handle['burstSize']['value'])):
                        readings = next(self.iter_sensor_data)
                        data_count += 1
                        # If we need to cherry pick cols, and possibly with a different name
                        if len(self.reading_cols) > 0:
                            for k, v in self.reading_cols.items():
                                if k in readings:
                                    burst_data_points.append({v: readings[k]})
                        else:
                            burst_data_points.append(readings)
                    sensor_data.update({"data": burst_data_points})
                    next_iteration_secs = self.period
                else:
                    readings = next(self.iter_sensor_data)
                    data_count += 1
                    # If we need to cherry pick cols, and possibly with a different name
                    if len(self.reading_cols) > 0:
                        for k, v in self.reading_cols.items():
                            if k in readings:
                                sensor_data.update({v: readings[k]})
                    else:
                        sensor_data.update(readings)
                    if self.handle['timestampFromFile']['value'] == 'true':
                        next_iteration_secs = self.get_time_stamp_diff(readings)
                    else:
                        next_iteration_secs = self.period
            except StopIteration as ex:
                _LOGGER.exception("playback - EOF reached: {}".format(str(ex)))
                # Rewind CSV file if it is to be read in an infinite loop
                if self.handle['repeatLoop']['value'] == 'true':
                    self.iter_sensor_data = iter(self.get_data())
                if data_count == 0:
                    return

            if not self.condition._is_owned():
                self.condition.acquire()
            value = {'data': sensor_data, 'ts': time_stamp}
            self.queue.put(value)
            if self.queue.full():
                self.condition.notify()
                self.condition.release()

            wait_event.wait(timeout=next_iteration_secs - (time.time()-time_start))


class Consumer(Thread):
    def __init__(self, queue, condition, handle, loop):
        super(Consumer, self).__init__()
        self.queue = queue
        self.condition = condition
        self.handle = handle
        self.loop = loop

    def run(self):
        asyncio.set_event_loop(loop=self.loop)
        while True:
            self.condition.acquire()
            while self.queue.qsize() > 0:
                data = self.queue.get()
                asyncio.ensure_future(self.save_data(data['data'], data['ts']), loop=self.loop)
            self.condition.notify()
            self.condition.release()

    async def save_data(self, sensor_data, time_stamp):
        try:
            data = {
                'asset': self.handle['assetName']['value'],
                'timestamp': time_stamp,
                'key': str(uuid.uuid4()),
                'readings': sensor_data
            }
            await Ingest.add_readings(asset='{}'.format(data['asset']),
                                      timestamp=data['timestamp'], key=data['key'],
                                      readings=data['readings'])
        except (asyncio.CancelledError, RuntimeError):
            pass
        except RuntimeWarning as ex:
            _LOGGER.exception("playback warning: {}".format(str(ex)))
        except Exception as ex:
            _LOGGER.exception("playback exception: {}".format(str(ex)))
            raise exceptions.DataRetrievalError(ex)
