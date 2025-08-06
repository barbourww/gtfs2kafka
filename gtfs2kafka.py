import threading
import traceback

import requests
import argparse
import time
from google.transit import gtfs_realtime_pb2
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
import zipfile
import io
import json
import time
import os

import kafka_confluent as kc

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()

class GTFSStaticProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_static"
        self.partition_key = "0"

        self.current_routes_trips = None
        self.current_stops = None
        
    def wait(self):
        time.sleep(self.config['GTFS_STATIC_UPDATE_MINS'] * 60)

    def pull_static_gtfs(self):
        pass

    def produce_static_route_trips_to_kafka(self):
        if self.current_routes_trips is not None:
            # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('service',b'gtfs'), ('datatype',b'routes')])
            pass

    def produce_static_stops_to_kafka(self):
        if self.current_stops is not None:
            # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('service',b'gtfs'), ('datatype',b'stops')])
            pass


class GTFSRTAlertsProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_alerts"
        self.partition_key = "0"

    def wait(self):
        time.sleep(self.config['GTFS_RT_ALERTS_UPDATE_SECS'])

    def receive_service_alerts(self):
        pass

    def produce_service_alerts(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'occupations')])
        pass


class GTFSRTTransitUpdatesProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_transit_updates"
        self.partition_key = "0"

    def wait(self):
        time.sleep(self.config['GTFS_RT_TRIPS_UPDATE_SECS'])

    def receive_transit_updates(self):
        pass

    def produce_transit_updates(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'objects')])
        pass


class GTFSRTVehicleProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_vehicles"
        self.partition_key = "0"

    def wait(self):
        time.sleep(self.config['GTFS_RT_VEHICLES_UPDATE_SECS'])

    def receive_vehicle_positions(self):
        pass

    def produce_vehicle_positions(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'zones')])
        pass


def update_gtfs_static(gtfs_static_config, receiver_kafka_config):
    static_receiver = GTFSStaticProducer(gtfs_static_config, kafka_config=receiver_kafka_config)
    while True:
        # 1) update with new data from static GTFS
        try:
            static_receiver.pull_static_gtfs()
        except Exception as e:
            logger.error("Failed to update GTFS static data within producer.")
            logger.exception(e, exc_info=True)
            # Don't produce to Kafka if we couldn't receive correctly.
            continue
        # 2) produce transit routes and trips to Kafka
        try:
            static_receiver.produce_static_route_trips_to_kafka()
        except Exception as e:
            logger.error("Failed to assemble and send static transit routes and trips to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) produce transit stops to Kafka
        try:
            static_receiver.produce_static_stops_to_kafka()
        except Exception as e:
            logger.error("Failed to assemble and send static transit stops to Kafka.")
            logger.exception(e, exc_info=True)
        # 4) invoke WAIT on the receiver object
        static_receiver.wait()

def update_gtfs_rt_alerts(gtfs_receiver_config, receiver_kafka_config):
    rt_alerts_receiver = GTFSRTAlertsProducer(gtfs_receiver_config, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of GTFS RT service alerts receiver.")
    while True:
        # 1) get the latest service alerts data
        try:
            rcv_data = rt_alerts_receiver.receive_service_alerts()
        except Exception as e:
            logger.error("Failed to pull updated service alerts.")
            logger.exception(e, exc_info=True)
            continue
        # 2) produce service alerts to Kafka
        try:
            rt_alerts_receiver.produce_service_alerts(packet=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send service alerts to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) invoke WAIT on the receiver object
        rt_alerts_receiver.wait()

def update_gtfs_rt_transit_updates(gtfs_receiver_config, receiver_kafka_config):
    rt_transit_receiver = GTFSRTTransitUpdatesProducer(gtfs_receiver_config, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of GTFS RT transit updates receiver.")
    while True:
        # 1) get the latest transit updates data
        try:
            rcv_data = rt_transit_receiver.receive_transit_updates()
        except Exception as e:
            logger.error("Failed to pull new transit service updates.")
            logger.exception(e, exc_info=True)
            continue
        # 2) produce transit updates to Kafka
        try:
            rt_transit_receiver.produce_transit_updates(packet=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send new transit service updates to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) invoke WAIT on the receiver object
        rt_transit_receiver.wait()

def update_gtfs_rt_vehicles(gtfs_receiver_config, receiver_kafka_config):
    rt_vehicles_receiver = GTFSRTVehicleProducer(gtfs_receiver_config, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of GTFS RT vehicles receiver.")
    while True:
        # 1) get the latest transit vehicles data
        try:
            rcv_data = rt_vehicles_receiver.receive_vehicle_positions()
        except Exception as e:
            logger.error("Failed to pull new transit vehicle positions.")
            logger.exception(e, exc_info=True)
            continue
        # 2) produce vehicle positions to Kafka
        try:
            rt_vehicles_receiver.produce_vehicle_positions(packet=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send new vehicle positions to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) invoke WAIT on the receiver object
        rt_vehicles_receiver.wait()

if __name__ == "__main__":
    static_config = {
        'GTFS_STATIC_URL': os.environ.get('GTFS_STATIC_URL'),
        'GTFS_STATIC_UPDATE_MINS': os.environ.get('GTFS_STATIC_UPDATE_MINS'),
    }

    rt_config = {
        'GTFS_RT_ALERTS_URL': os.environ.get('GTFS_RT_ALERTS_URL'),
        'GTFS_RT_ALERTS_UPDATE_SECS': os.environ.get('GTFS_RT_ALERTS_UPDATE_SECS'),

        'GTFS_RT_TRIPS_URL': os.environ.get('GTFS_RT_TRIPS_URL'),
        'GTFS_RT_TRIPS_UPDATE_SECS': os.environ.get('GTFS_RT_TRIPS_UPDATE_SECS'),

        'GTFS_RT_VEHICLES_URL': os.environ.get('GTFS_RT_VEHICLES_URL'),
        'GTFS_RT_VEHICLES_UPDATE_SECS': os.environ.get('GTFS_RT_VEHICLES_UPDATE_SECS'),
    }
    
    common_kafka_config = {
        'KAFKA_BOOTSTRAP': os.environ.get('KAFKA_BOOTSTRAP'),
        'KAFKA_USER':  os.environ.get('KAFKA_USER'),
        'KAFKA_PASSWORD': os.environ.get('KAFKA_PASSWORD'),
    }
    
    log_path = str(os.environ.get('LOG_PATH')) if os.environ.get('LOG_PATH') else "."
    loggerFile = log_path + '/gtfs2kafka.log'
    print('Saving logs to: ' + loggerFile)
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(filename=loggerFile, level=logging.INFO, format=FORMAT)

    logger.info("Starting 4x GTFS to Kafka producer threads.")
    threading.Thread(target=update_gtfs_static, args=(static_config, common_kafka_config))
    threading.Thread(target=update_gtfs_rt_alerts, args=(rt_config, common_kafka_config))
    threading.Thread(target=update_gtfs_rt_vehicles, args=(rt_config, common_kafka_config))
    threading.Thread(target=update_gtfs_rt_transit_updates, args=(rt_config, common_kafka_config))
        
