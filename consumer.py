#! /usr/bin/python

"""
Simple stream consumer

"""

import sys
import random
import time
import json
import os
import argparse
import logging
import settings

from confluent_kafka import Consumer, KafkaError

parser = argparse.ArgumentParser(description='Consume a stream')
parser.add_argument('--path',help='stream path')
parser.add_argument('--group',help='consumer group', default=str(time.time()))
parser.add_argument('--topic',help='topic', default="default_topic")
args = parser.parse_args()

if not args.path:
    print("--path required")
    sys.exit()

stream = args.path
topic = args.topic
group = args.group


consumer = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'latest'}})
consumer.subscribe(["{}:{}".format(stream,topic)])

running = True
while running:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        pass
    else:
        if not msg.error():
            document = json.loads(msg.value().decode("utf-8"))
            print(document)
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print("error : {}".format(msg.error()))
            running = False
        else:
            pass
