#! /usr/bin/python

"""
Traffic generator for mapr-global-stream-demo

generates events 

events are pushed to a MapR stream named as the country passed in argument

"""

import sys
import random
import time
import json
import os
import argparse
import logging
import settings

from confluent_kafka import Producer 

parser = argparse.ArgumentParser(description='Launch a car stream producer')
parser.add_argument('--country',help='collector country')
parser.add_argument('--traffic',help='city traffic (cars/sec)', default=10)
parser.add_argument('--reset',help='delete historical data',action="store_true")
args = parser.parse_args()

if not args.country:
    print("--country required")
    sys.exit()

country = args.country
traffic = int(args.traffic)

logging.basicConfig(filename = settings.LOG_FOLDER + 'carwatch_' + country + '.log',level=logging.INFO)


stream = settings.SOURCE_FOLDER + country

# Deletes stream if reset required
if args.reset :
    settings.CLUSTER.delete_stream(stream)

# Test if stream exists
if not settings.CLUSTER.is_stream(stream):
    logging.debug("creating stream {}".format(stream))
    settings.CLUSTER.create_stream(stream)
    logging.debug("stream created")





logging.debug("creating producer for {}".format(stream))
p = Producer({'streams.producer.default.stream': stream})



def generate_models_distribution():
    # Load cars models from json file
    carfile = settings.DATA_FOLDER + "cars.json"
    models = []
    with open(carfile) as f:
        for line in f:
            while True:
                try:
                    car = json.loads(line)
                    break
                except ValueError:
                    # Not yet a complete JSON value
                    line += next(f)
            models.append(car["model"])

    logging.debug("Models loaded:")
    logging.debug(models)


    # Generate car distribution
    model_distrib = []
    for model in models:
        rand = random.randint(0,100)
        if 15<rand<85:
            for i in range(rand):
                model_distrib.append(model)

    return model_distrib


def generate_color_distribution():
    # Generate color distribution
    colors = []
    colors.append(("black",random.randint(0,30)))
    colors.append(("white",random.randint(0,30)))
    colors.append(("grey",random.randint(0,30)))
    colors.append(("blue",random.randint(0,30)))
    colors.append(("red",random.randint(0,30)))
    colors.append(("brown",random.randint(0,30)))
    colors.append(("green",random.randint(0,30)))

    color_distrib = []
    for color in colors:
        weight = color[1]
        for i in range(weight):
            color_distrib.append(color[0])

    return color_distrib


model_distrib = generate_models_distribution()
color_distrib = generate_color_distribution()


logging.debug("Injecting ...")

nb_cars = 0
while True:
    car_model = model_distrib[random.randint(0,len(model_distrib)-1)]
    car_color = color_distrib[random.randint(0,len(color_distrib)-1)]
    message = {"timestamp":int(time.time()),"country":country,"color":car_color,"model":car_model}
    p.produce("default_topic", json.dumps(message))
    time.sleep(1/traffic)
    nb_cars += 1
    if nb_cars % (traffic * 10) == 0:
        logging.info("{} cars injected".format(nb_cars))
        model_distrib = generate_models_distribution()
        color_distrib = generate_models_distribution()



