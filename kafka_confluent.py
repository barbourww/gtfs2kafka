import asyncio
import json


#from kafka import KafkaProducer
#from kafka.admin import KafkaAdminClient, NewTopic

from confluent_kafka import Producer


class KafkaConfluentHelper():
    def __init__(self, config):
#        self.conf = config
    
        self.conf = {
            'bootstrap.servers': config['KAFKA_BOOTSTRAP'],
            'security.protocol': 'SASL_SSL',
        #    'ssl.ca.location': 'kafka-cluster-ca.crt',
            'ssl.ca.location': 'strimzi-ca.crt',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': config['KAFKA_USER'],
            'sasl.password': config['KAFKA_PASSWORD'],
            'ssl.endpoint.identification.algorithm': 'none'
        }

        self.producer = Producer(self.conf)

        # Fetch and print broker metadata
        md = self.producer.list_topics(timeout=10)

        print("Brokers:")
        for broker in md.brokers.values():
            print(f"- id: {broker.id}, host: {broker.host}, port: {broker.port}")

    def on_send_success(self, record_metadata):
        #print(record_metadata.topic)
        #print(record_metadata.partition)
        #print(record_metadata.offset)
        pass

    def on_send_error(self, excp):
        log.error('I am an errback', exc_info=excp)
        
    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Delivery failed: {err}')
#        else:
#            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            
            
    def send(self, topic, key,  json_data, partition=None, headers=None):
    #    producer.send(topic, key=key.encode('utf-8'), value=json.dumps(json_data).encode('utf-8'),
    #    partition=partition, headers=headers).add_callback(on_send_success).add_errback(on_send_error)
        
        self.producer.produce(topic, key=key.encode('utf-8'), value=json.dumps(json_data).encode('utf-8'), headers=headers, callback=self.delivery_report)
        self.producer.poll(0)
        
    #    producer = EventHubProducerClient.from_connection_string(
    #        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    #    )
    #
    #    async with producer:
    #        # Create a batch.
    ##        event_data_batch = await producer.create_batch()
    #
    #        # Add events to the batch.
    #        print(json.dumps(packaged_data))
    ##        event_data_batch.add(EventData(json.dumps(packaged_data)))
    #
    #        # Send the batch of events to the event hub.
    ##        await producer.send_batch(event_data_batch)
    #
    #        await producer.send_event(event_data=EventData(json.dumps(packaged_data)), partition_key=packaged_data['segment_id'])

if __name__ == "__main__":
    new_data = {
        'test': "hello"
    }
    packaged_data = package(new_data)
#    print(packaged_data)
    
#    asyncio.run(send(packaged_data))
    headers = [('key1', b'value1'), ('key2', b'value2')]
    send(topic='postgres-data', key='my_key', partition=0, json_data=packaged_data, headers=headers)
    producer.flush()
    
