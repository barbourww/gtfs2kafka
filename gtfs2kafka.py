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

import sys

import kafka_confluent as kc

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()


# Helper function to wrap thread targets for fatal error handling
def thread_wrapper(target_func, args=(), name=""):
    def wrapped():
        try:
            target_func(*args)
        except Exception:
            logger.critical(f"Unhandled exception in thread '{name}', exiting entire process.")
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    return wrapped


class Stop:
    def __init__(self, stop_id, name, lat, lon):
        self.stop_id = stop_id
        self.name = name
        self.lat = lat
        self.lon = lon
        self.routes = set()


class Trip:
    def __init__(self, trip_id, service_id, route_id, shape_id, stops):
        self.trip_id = trip_id
        self.service_id = service_id
        self.route_id = route_id
        self.shape_id = shape_id
        self.stops = stops  # list of Stop in sequence


class Route:
    def __init__(self, route_id, short_name, long_name, color, trips, shape_geometry):
        self.route_id = route_id
        self.short_name = short_name
        self.long_name = long_name
        self.color = color
        self.trips = trips  # list of Trip objects
        self.shape_geometry = shape_geometry


class GTFSStaticProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_static"
        self.partition_key = "0"

        self.routes_dict = None
        self.trips_dict = None
        self.stops_dict = None

    def wait(self):
        time.sleep(self.config['GTFS_STATIC_UPDATE_MINS'] * 60)

    def pull_static_gtfs(self):
        def download_gtfs(url):
            resp = requests.get(url)
            resp.raise_for_status()
            return zipfile.ZipFile(io.BytesIO(resp.content))

        def load_gtfs_table(gtfs_zip, filename):
            return pd.read_csv(gtfs_zip.open(filename))

        gtfs_zip = download_gtfs(self.config['GTFS_STATIC_URL'])
        routes = load_gtfs_table(gtfs_zip, 'routes.txt')
        trips = load_gtfs_table(gtfs_zip, 'trips.txt')
        shapes = load_gtfs_table(gtfs_zip, 'shapes.txt')
        stop_times = load_gtfs_table(gtfs_zip, 'stop_times.txt')
        stops = load_gtfs_table(gtfs_zip, 'stops.txt')

        # join trips with routes
        trips_with_routes = trips.merge(
            routes[['route_id', 'route_short_name', 'route_long_name', 'route_color']],
            on='route_id', how='left'
        )

        # Build geometry per shape_id
        shape_lines = (
            shapes
            .sort_values(['shape_id', 'shape_pt_sequence'])
            .groupby('shape_id')
            .apply(lambda grp: LineString(zip(grp.shape_pt_lon, grp.shape_pt_lat)))
            .rename('geometry')
            .reset_index()
        )
        shapes_gdf = gpd.GeoDataFrame(shape_lines, geometry='geometry', crs='EPSG:4326')

        stop_times_sorted = stop_times.sort_values(['trip_id', 'stop_sequence'])
        stops_full = stop_times_sorted.merge(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']],
                                             on='stop_id', how='left')
        self.stops_dict = {row.stop_id: Stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)
                           for _, row in stops.iterrows()}

        # build a dictionary of shapes
        shape_geom = dict(zip(shapes_gdf.shape_id, shapes_gdf.geometry))

        # stops per trip
        trip_to_stops = {
            trip_id: [
                Stop(row.stop_id, row.stop_name, row.stop_lat, row.stop_lon)
                for idx, row in grp.iterrows()
            ]
            for trip_id, grp in stops_full.groupby('trip_id')
        }

        # build Trip objects
        self.trips_dict = {}
        for _, row in trips_with_routes.iterrows():
            self.trips_dict[row.trip_id] = Trip(
                trip_id=row.trip_id,
                service_id=row.service_id,
                route_id=row.route_id,
                shape_id=row.shape_id,
                stops=trip_to_stops.get(row.trip_id, [])
            )

        # Add route associations to each stop
        for trip in self.trips_dict.values():
            for stop in trip.stops:
                if stop.stop_id in self.stops_dict:
                    self.stops_dict[stop.stop_id].routes.add(trip.route_id)

        # build Route objects
        self.routes_dict = {}
        for route_id, grp in trips_with_routes.groupby('route_id'):
            any_trip = grp.iloc[0]
            shape_id = any_trip.shape_id
            self.routes_dict[route_id] = Route(
                route_id=route_id,
                short_name=any_trip.route_short_name,
                long_name=any_trip.route_long_name,
                color=any_trip.get('route_color', None),
                trips=[self.trips_dict[t] for t in grp.trip_id],
                shape_geometry=shape_geom.get(shape_id, None)
            )

    def produce_static_routes_to_kafka(self):
        if self.routes_dict is not None:
            for route in self.routes_dict.values():
                payload = {
                    "route_id": route.route_id,
                    "short_name": route.short_name,
                    "long_name": route.long_name,
                    "color": route.color,
                    "shape_wkt": route.shape_geometry.wkt if route.shape_geometry is not None else None,
                    "shape_points": list(route.shape_geometry.coords) if route.shape_geometry is not None else [],
                    "trips": [
                        {
                            "trip_id": t.trip_id,
                            "service_id": t.service_id,
                            "shape_id": t.shape_id,
                            "stops": [
                                {"stop_id": s.stop_id, "name": s.name, "lat": s.lat, "lon": s.lon}
                                for s in t.stops
                            ]
                        } for t in route.trips
                    ]
                }
                self.kc.send(topic=self.topic_name, key=self.partition_key,
                             json_data=json.dumps(payload),
                             headers=[('service', b'gtfs'), ('datatype', b'routes')])
        else:
            logger.error("Static routes not loaded yet!")

    def produce_static_trips_to_kafka(self):
        if self.trips_dict is not None:
            for trip in self.trips_dict.values():
                payload = {
                    "trip_id": trip.trip_id,
                    "service_id": trip.service_id,
                    "route_id": trip.route_id,
                    "shape_id": trip.shape_id,
                    "stops": [
                        {"stop_id": s.stop_id, "name": s.name, "lat": s.lat, "lon": s.lon}
                        for s in trip.stops
                    ]
                }
                self.kc.send(topic=self.topic_name, key=self.partition_key,
                             json_data=json.dumps(payload),
                             headers=[('service', b'gtfs'), ('datatype', b'trips')])
        else:
            logger.error("Static trips not loaded yet!")

    def produce_static_stops_to_kafka(self):
        if self.stops_dict is not None:
            for stop in self.stops_dict.values():
                payload = {
                    "stop_id": stop.stop_id,
                    "name": stop.name,
                    "lat": stop.lat,
                    "lon": stop.lon,
                    "routes": list(stop.routes)
                }
                self.kc.send(topic=self.topic_name, key=self.partition_key,
                             json_data=json.dumps(payload),
                             headers=[('service', b'gtfs'), ('datatype', b'stops')])
        else:
            logger.error("Static stops not loaded yet!")



class GTFSRTAlertsProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_alerts"
        self.partition_key = "0"

    def wait(self):
        time.sleep(self.config['GTFS_RT_ALERTS_UPDATE_SECS'])

    def receive_service_alerts(self):
        response = requests.get(self.config['GTFS_RT_ALERTS_URL'])
        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {self.config['GTFS_RT_ALERTS_URL']}, "
                                  f"status code {response.status_code}")
        feed_data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(feed_data)

        alerts_dict = {}
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert_id = entity.id
            alert = entity.alert

            # Active periods
            active_periods = [
                {"start": period.start, "end": period.end}
                for period in alert.active_period
            ]

            # Collect all informed entities
            affected_routes = set()
            affected_stops = set()
            for informed in alert.informed_entity:
                if informed.route_id:
                    affected_routes.add(informed.route_id)
                if informed.stop_id:
                    affected_stops.add(informed.stop_id)

            if alert.HasField("cause"):
                cause = alert.cause
            else:
                cause = None
            if alert.HasField("effect"):
                effect = alert.effect
            else:
                effect = None

            # Text fields
            def extract_text(translation_list):
                for translation in translation_list:
                    if translation.language == "en":
                        return translation.text
                return ""

            header = extract_text(alert.header_text.translation)
            description = extract_text(alert.description_text.translation)

            alerts_dict[alert_id] = {
                "id": alert_id,
                "active_period": active_periods,
                "affected_routes": list(affected_routes),
                "affected_stops": list(affected_stops),
                "cause": cause,
                "effect": effect,
                "header": header,
                "description": description
            }

        return alerts_dict

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

    def receive_trip_updates(self, response_content):
        response = requests.get(self.config['GTFS_RT_TRIPS_UPDATE_URL'])
        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {self.config['GTFS_RT_TRIPS_UPDATE_URL']}, "
                                  f"status code {response.status_code}")
        feed_data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(feed_data)

        updates_dict = {}
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            update = entity.trip_update
            trip = update.trip
            vehicle = update.vehicle

            trip_id = trip.trip_id
            update_id = entity.id

            stop_updates = []
            for stu in update.stop_time_update:
                relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                    stu.schedule_relationship
                ) if stu.HasField("schedule_relationship") else "SCHEDULED"

                if relationship == "SKIPPED":
                    stop_info = {
                        "stop_sequence": stu.stop_sequence,
                        "stop_id": stu.stop_id,
                        "status": "SKIPPED"
                    }
                else:
                    time_val = None
                    if stu.HasField("departure") and stu.departure.HasField("time"):
                        time_val = stu.departure.time
                    elif stu.HasField("arrival") and stu.arrival.HasField("time"):
                        time_val = stu.arrival.time

                    stop_info = {
                        "stop_sequence": stu.stop_sequence,
                        "stop_id": stu.stop_id,
                        "status": "DEPARTURE" if stu.HasField("departure") else "ARRIVAL",
                        "time": time_val
                    }

                stop_updates.append(stop_info)

            updates_dict[update_id] = {
                "id": update_id,
                "trip_id": trip_id,
                "start_time": trip.start_time,
                "start_date": trip.start_date,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "vehicle_id": vehicle.id if vehicle else None,
                "stop_updates": stop_updates,
                "timestamp": update.timestamp if update.HasField("timestamp") else None
            }

        return updates_dict

    def produce_transit_updates(self, packet):
        # self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'objects')])
        pass


class GTFSRTVehicleProducer:
    def __init__(self, receiver_config, kafka_config):
        self.config = receiver_config
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "gtfs_vehicles"

        self.filter_bbox = {
            'min_lat': 35.7,
            'max_lat': 36.6,
            'min_lon': -87.5,
            'max_lon': -86.25
        }

    def wait(self):
        time.sleep(self.config['GTFS_RT_VEHICLES_UPDATE_SECS'])

    def receive_vehicle_positions(self):
        response = requests.get(self.config['GTFS_RT_VEHICLES_URL'])
        if response.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {self.config['GTFS_RT_VEHICLES_URL']}, "
                                  f"status code {response.status_code}")
        feed_data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(feed_data)
        vehicle_positions = []

        for entity in feed.entity:
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                position = vehicle.position
                trip = vehicle.trip

                if vehicle.HasField('occupancy_percentage'):
                    occupancy = int(vehicle.occupancy_percentage)
                else:
                    occupancy = None

                if self.filter_bbox is not None and len(self.filter_bbox) > 0:
                    if not (self.filter_bbox['min_lat'] <= position.latitude <= self.filter_bbox['max_lat'] and
                            self.filter_bbox['min_lon'] <= position.longitude <= self.filter_bbox['max_lon']):
                        continue  # Skip vehicles outside the bounding box

                vehicle_positions.append({
                    'id': entity.id,
                    'latitude': position.latitude,
                    'longitude': position.longitude,
                    'bearing': position.bearing,
                    'speed': position.speed,
                    'route_id': trip.route_id,
                    'trip_id': trip.trip_id,
                    'occupancy_pct': occupancy,
                    'timestamp': vehicle.timestamp
                })

        return vehicle_positions

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
    threading.Thread(target=thread_wrapper(update_gtfs_static, args=(static_config, common_kafka_config), name="gtfs_static")).start()
    threading.Thread(target=thread_wrapper(update_gtfs_rt_alerts, args=(rt_config, common_kafka_config), name="gtfs_alerts")).start()
    threading.Thread(target=thread_wrapper(update_gtfs_rt_vehicles, args=(rt_config, common_kafka_config), name="gtfs_vehicles")).start()
    threading.Thread(target=thread_wrapper(update_gtfs_rt_transit_updates, args=(rt_config, common_kafka_config), name="gtfs_transit_updates")).start()
        
