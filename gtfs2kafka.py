import logging
logger = logging.getLogger(__name__)
import argparse

import time
import json
import os

import kafka_confluent as kc

from dotenv import load_dotenv
load_dotenv()

class LaddmsRecorder():
    def __init__(self, cfg, kc):
        self.cfg = cfg
        self.kc = kc
        self.ousterObjectSubscriber = ouster.Ouster(host=cfg['OUSTER_HOST'], port=3302)
        self.ousterObjectSubscriber.packet_callback = self.newOusterObjectPacket
            
#        self.ousterOccupationSubscriber = ouster.Ouster(host=cfg['OUSTER_HOST'], port=3303)
#        self.ousterOccupationSubscriber.packet_callback = self.newOusterOccupationPacket
        
        self.objectPacketCount = 0
        self.zonePacketCount = 0
        
        self.device_id = cfg['INTERSECTION_ID']
        
        self.topic_name = "unassinged"
        self.partition_key = "0"
        

    def start(self):
#        self.sql_connection = db_laddms.make_sql_connection(self.cfg)
#        self.sql_cursor = db_laddms.make_sql_cursor(self.sql_connection)
        self.update_id()
        
        self.ousterObjectSubscriber.start()
#        self.ousterOccupationSubscriber.start()
        
        
    def update_id(self):
        # Client ID is always "detect-client"
        client_id = "detect-client" #self.cfg['OUSTER_USER'] # HACK temp fix
        client_secret = self.cfg['OUSTER_SECRET']
        # Base URL is the IP address or domain name of the server hosting Gemini Detect
        base_url = self.cfg['OUSTER_TCP_URL']
        # Token URL should always be as shown below
        token_url = f"{base_url}/auth/realms/detect/protocol/openid-connect/token"
    
        api = gemini.AuthenticatedAPI(client_id, client_secret, token_url, base_url)
#        response = api.get("perception/api/v1/settings")
        response = api.get("lidar-hub/api/v1/diagnostics")
#        print( str(response))
#        print(json.dumps(response, indent=3))
#        print(response['hardware_id'])
#        print(response['compute']['host_name'])
#        print(response['compute']['mac_address'])
#        self.partition_key = response['hardware_id'].replace("*", "")
        self.partition_key = str(self.device_id)
#        self.topic = 'postgres-data'
        self.topic = 'my-topic'
        
        
    def update_zone_info(self):
        # Client ID is always "detect-client"
        client_id = "detect-client"
        # Client secret is retreived from the Keycloak Admin Console -> Clients(side bar) ->
        # detect-client(clients list) -> Credentials (tab)
        client_secret = self.cfg['OUSTER_SECRET']
        # Base URL is the IP address or domain name of the server hosting Gemini Detect
        #base_url = "https://192.168.2.3:4443"
        base_url = self.cfg['OUSTER_TCP_URL']
        # Token URL should always be as shown below
        token_url = f"{base_url}/auth/realms/detect/protocol/openid-connect/token"

        api = gemini.AuthenticatedAPI(client_id, client_secret, token_url, base_url)
        response = api.get("lidar-hub/api/v1/event_zones")
#        print(json.dumps(response, indent=2))
#        with open("event_zones.txt", "w") as text_file:
#            text_file.write(json.dumps(response, indent=2))
#        pass
#        db_laddms.insert_event_zones(json_data=response, device_id=self.device_id, use_db_cursor=self.sql_cursor)
        self.kc.send(topic=self.topic, key=self.partition_key, json_data=response, headers=[('device',b'ouster'), ('detection',b'zones')])
        

    def newOusterObjectPacket(self, packet):
#        self.objectPacketCount += 1
#        if self.objectPacketCount >= 100:
#            self.objectPacketCount = 0
#            print("Got 100 object packets!")
#        db_laddms.insert_object_detections(json_data=packet, intersection_id=self.cfg['INTERSECTION_ID'], device_id=self.device_id, use_db_cursor=self.sql_cursor)
#        print("Objects inserted at " + str(time.time()))
#        logger.info("New PAcket!")
        self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'objects')])
        
    def newOusterOccupationPacket(self, packet):
#        self.zonePacketCount += 1
#        if self.zonePacketCount >= 100:
#            self.zonePacketCount = 0
#            print("Got 100 zone packets!")
#        db_laddms.insert_zone_occupation(json_data=packet, device_id=self.device_id, use_db_cursor=self.sql_cursor)
#        print("Occupations inserted at " + str(time.time()))
        self.kc.send(topic=self.topic, key=self.partition_key, json_data=packet, headers=[('device',b'ouster'), ('detection',b'occupations')])
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ouster to Kafka')
    parser.add_argument('-i', '--intersection_id', default=None)
    parser.add_argument('-o', '--host', default=None)
    parser.add_argument('-s', '--secret', default=None)
    parser.add_argument('-d', '--device', default=None)
    
    parser.add_argument('-k', '--kafkahost', default=None)
    parser.add_argument('-u', '--kafkauser', default=None)
    parser.add_argument('-p', '--kafkapassword', default=None)
    args = parser.parse_args()
    
    if not args.host and not os.environ.get('OUSTER_HOST'):
        print("Error, OUSTER_HOST not defined!")
        quit()
    url = ("https://" + str(args.host) + ":443") if args.host else ("https://" + os.environ.get('OUSTER_HOST') + ":443")
    config = {
        'OUSTER_HOST': args.host if args.host else os.environ.get('OUSTER_HOST'),
        'OUSTER_TCP_URL': url,
        'OUSTER_SECRET': args.secret if args.secret else os.environ.get('OUSTER_SECRET'),
        'DEVICE_ID': args.device if args.device else os.environ.get('DEVICE_ID'),
        'INTERSECTION_ID': args.intersection_id if args.intersection_id else int(os.environ.get('INTERSECTION_ID')),
    }
    
#    print(config)
#    quit()
    
    config_kafka = {
        'KAFKA_BOOTSTRAP': args.kafkahost if args.kafkahost else os.environ.get('KAFKA_BOOTSTRAP'),
        'KAFKA_USER':  args.kafkauser if args.kafkauser else os.environ.get('KAFKA_USER'),
        'KAFKA_PASSWORD': args.kafkapassword if args.kafkapassword else os.environ.get('KAFKA_PASSWORD'),
    }
    print(config_kafka)
    
    log_path = str(os.environ.get('LOG_PATH')) if os.environ.get('LOG_PATH') else "."
    
    kafkaConfluent = kc.KafkaConfluentHelper(config_kafka)
    
    loggerFile = log_path + '/ouster2kafka-' + str(config['INTERSECTION_ID']) + '.log'
    print('Saving logs to: ' + loggerFile)
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(filename=loggerFile, level=logging.INFO, format=FORMAT)

    logger.info("Running... for ouster at " + config['OUSTER_HOST'])
    logger.info("Intersection Id: " + str(config['INTERSECTION_ID']))

    myLaddmsRecorder = LaddmsRecorder(config, kafkaConfluent)
    myLaddmsRecorder.start()
        
    while True:
        myLaddmsRecorder.update_zone_info()
        time.sleep(60)
        
