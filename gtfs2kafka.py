import threading

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

class GTFSStaticProducer():
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "unassinged"
        self.partition_key = "0"
        

    def pull_static_gtfs(self):
        pass

    def produce_static_route_trips_to_kafka(self):
        pass

    def produce_static_stops_to_kafka(self):
        pass


class GTFSRTVehicleProducer():
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "unassinged"
        self.partition_key = "0"

    def receive_vehicle_positions(self):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=response, headers=[('device',b'ouster'), ('detection',b'zones')])
        pass


class GTFSRTTransitUpdatesProducer():
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "unassinged"
        self.partition_key = "0"

    def receive_transit_updates(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'objects')])
        pass


class GTFSRTAlertsProducer():
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "unassinged"
        self.partition_key = "0"

    def receive_service_alerts(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'occupations')])
        pass


def update_gtfs_static():
    pass

def update_gtfs_rt_alerts():
    pass

def update_gtfs_rt_transit_updates():
    pass

def update_gtfs_rt_vehicles():
    pass

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

    logger.info("Starting GTFS to Kafka producer.")

    static_receiver = GTFSStaticProducer(static_config, kafka_config=common_kafka_config)
    rt_alerts_receiver = GTFSRTAlertsProducer(rt_config, kafka_config=common_kafka_config)
    rt_transit_receiver = GTFSRTTransitUpdatesProducer(rt_config, kafka_config=common_kafka_config)
    rt_vehicles_receiver = GTFSRTVehicleProducer(rt_config, kafka_config=common_kafka_config)

        
    while True:
        threading.Thread(target=update_gtfs_static)
        threading.Thread(target=update_gtfs_rt_alerts)
        threading.Thread(target=update_gtfs_rt_vehicles)
        threading.Thread(target=update_gtfs_rt_transit_updates)
        
