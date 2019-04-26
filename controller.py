#! /usr/bin/python

"""
mapr-global-streaming-demo
demonstrates MapR ability to replicate live streams across the global namespace
main script
"""

import os
import subprocess
import threading
import json
import time
import random
import logging
import signal
import traceback

import mapr_manager

from flask import Flask, render_template, request
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory
from confluent_kafka import Consumer, KafkaError

import settings


logging.basicConfig(filename='logs/controller.log',level=logging.INFO)

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

def handler(signum, frame):
    """ handler to deal properly with ctrl-Z """ 
    print('Ctrl+Z pressed, shutting down server')
    shutdown_server()

signal.signal(signal.SIGTSTP, handler)


# Global variables
consumers = {}  # List of active stream consumers

# Initialize databases
if settings.SECURE_MODE:
  connection_str = "{}:5678?auth=basic;" \
                           "user={};" \
                           "password={};" \
                           "ssl=true;" \
                           "sslCA={};" \
                           "sslTargetNameOverride={}".format(settings.CLUSTER_IP,
                                                             settings.USERNAME,
                                                             settings.PASSWORD,
                                                             settings.PEM_FILE,
                                                             settings.CLUSTER_IP)

  cluster = mapr_manager.Cluster(settings.CLUSTER_NAME,settings.CLUSTER_IP,settings.USERNAME,settings.PASSWORD)

else:
  connection_str = "{}:5678?auth=basic;user={};password={};ssl=false".format(settings.CLUSTER_IP,
                                                                             settings.USERNAME,
                                                                             settings.PASSWORD)
  cluster = mapr_manager.Cluster(settings.CLUSTER_NAME,settings.CLUSTER_IP,settings.USERNAME,settings.PASSWORD)


def list_streams(path,return_fullpath=False):
  """ returns a list with the name or full path of all streams available in the given path """
  streams = []
  for filename in os.listdir(path):
    if os.path.islink(path + filename):
      if cluster.is_stream(path + filename):
        if return_fullpath:
          streams.append(path + filename)
        else:
          streams.append(filename)
  return streams

def delete_streams(streams_path):
  """ delete all streams available in the stream path """
  try:
    for filename in os.listdir(streams_path):
      if os.path.islink(streams_path + filename):
        if cluster.is_stream(streams_path + filename):
          cluster.delete_stream(streams_path + filename)
    return True
  except:
    logging.ERROR(traceback.format_exc())
    return False


app = Flask(__name__)


######  Web pages  #####

@app.route('/')
def home():
  return render_template('controller.html',countries=["north-america","europe","australia"])


@app.route('/get_streams_data',methods = ['POST'])
def get_streams_data(): 
  """ Returns all stream data since last poll """
  global consumers

  scope = request.form["scope"]

  # data results for each stage
  raw_data = {}
  count_data = {}
  stream_data = {}

  # Poll new vehicles from all the streams
  for stream, consumer in consumers.items():
    process = ("_replica" in stream and scope != "source") or ("_replica" not in stream and scope != "target")
    if process:
      raw_data[stream] = {}
      running = True
      logging.info("polling {}".format(stream))
      while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
          running = False
        else:
          if not msg.error():
            document = json.loads(msg.value().decode("utf-8"))
            model = document["model"]
            if model in raw_data[stream]:
              raw_data[stream][model] += 1
            else:
              raw_data[stream][model] = 1
          elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
          else:
            # No more messages
            running = False

  for stream,data in raw_data.items():
    count_data[stream.split('/')[-1]] = data

  stream_data = count_data

  return json.dumps(stream_data)



@app.route('/get_streams',methods = ['POST'])
def get_streams():
  """ Returns the list of all existing streams in the data folder """
  return json.dumps(list_streams(settings.SOURCE_FOLDER) + list_streams(settings.TARGET_FOLDER))



@app.route('/deploy_new_country',methods=['POST'])
def deploy_country():
  global consumers
  new_country = request.form['country']
  stream_path = settings.SOURCE_FOLDER + new_country
  if not settings.CLUSTER.is_stream(stream_path):
    logging.info("deploying {}".format(new_country))
    subprocess.Popen(['python','traffic_generator.py','--country',new_country,'--traffic',str(random.randint(10,100))])

    # Generating unique group ID to avoid collisions
    group = str(time.time())

    # Create a new consumer and store it in the "consumers" global variable
    consumers[new_country] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
    
    # Wait until the stream is ready
    while not settings.CLUSTER.is_stream(stream_path):
      time.sleep(0.5)

    # subcribe to the default topic for the stream
    consumers[new_country].subscribe(["{}:default_topic".format(stream_path)])
    
    logging.info("{} deployed".format(new_country))
    return "{} deployed".format(new_country)

  return "{} already deployed".format(new_country)


@app.route('/replicate_streams') 
def replicate_streams():
  """ Replicate all existing source streams to the target directory """

  # Get all the source streams
  source_streams_names = list_streams(path=settings.SOURCE_FOLDER)
  
  for source_stream_name in source_streams_names:
    source_stream = settings.SOURCE_FOLDER + source_stream_name
    replica_stream_name = source_stream_name + "_replica"
    replica_stream = settings.TARGET_FOLDER + replica_stream_name

    # If replica doesn't exist yet we create it
    if not settings.CLUSTER.is_stream(replica_stream):
      settings.CLUSTER.replicate_stream(path=source_stream,replica=replica_stream)

      # Generating unique group ID to avoid collisions
      group = str(time.time())

      # Create a consumer to the new replica
      consumers[replica_stream_name] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})

      # Wait until the stream is ready
      while not settings.CLUSTER.is_stream(replica_stream):
        time.sleep(0.5)

      # subcribe to the default topic for the stream
      consumers[replica_stream_name].subscribe(["{}:default_topic".format(replica_stream)])

  return "Streams replicated"



@app.route('/remove_country',methods=['POST'])
def remove_country():
  global consumers

  # get country to delete from the request
  country = request.form['country']

  source_stream_name = country
  replica_stream_name = country + "_replica"
  
  # Close and delete the consumer from the global list of active consumers
  consumer[source_stream_name].close()
  del consumers[source_stream_name]

  # Deletes the stream on the cluster
  settings.CLUSTER.delete_stream(settings.SOURCE_FOLDER + source_stream_name)

  # Close and delete the replica consumer
  consumers[replica_stream_name].close()
  del consumers[replica_stream_name]

  # Deletes the replica stream on the cluster
  settings.CLUSTER.delete_stream(settings.TARGET_FOLDER + replica_stream_name)
  
  return "{} removed".format(country)



if __name__ == '__main__':
  # Deletes all existing streams at startup
  delete_streams(settings.SOURCE_FOLDER)
  delete_streams(settings.TARGET_FOLDER)
  app.run(debug=False,host='0.0.0.0',port=80)

