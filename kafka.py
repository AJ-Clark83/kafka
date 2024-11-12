# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 12:03:21 2024

@author: andrew.clark
"""

import json
from confluent_kafka import Consumer, KafkaException

# Kafka configuration details
bootstrap_servers = "b-1-public.testcluster1.dxsxco.c4.kafka.ap-southeast-2.amazonaws.com:9196,b-2-public.testcluster1.dxsxco.c4.kafka.ap-southeast-2.amazonaws.com:9196"
sasl_username = "maca"
sasl_password = "t:S*-N8GNEkS*>E"
sasl_mechanism = "SCRAM-SHA-512"
security_protocol = "SASL_SSL"
group_id = "maca"
topics = ["Maca-Mining", "MACA-Mining-Karlawinda", "MACA-Mining-Ravensthorpe", "Maca-Mining-Sanjiv-Ridge"]

# Configure the Kafka Consumer
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanisms': sasl_mechanism,
    'sasl.username': sasl_username,
    'sasl.password': sasl_password,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe(topics)

# Open the JSON file in append mode to store messages
with open("kafka_events.json", "a") as file:
    try:
        print("Connected to Kafka. Listening for messages...")
        while True:
            msg = consumer.poll(1.0)  # Timeout set to 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue  # End of partition event
                else:
                    print("Error:", msg.error())
                    break
            else:
                # Prepare the event data
                event = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "value": msg.value().decode('utf-8')
                }
                
                # Write the event data as a JSON line to the file
                file.write(json.dumps(event) + "\n")
                
                # Print confirmation (optional, can be removed in production)
                print(f"Stored message from {event['topic']} partition {event['partition']} offset {event['offset']}")

    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        # Clean up the Kafka consumer
        consumer.close()