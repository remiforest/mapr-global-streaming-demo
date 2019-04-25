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



connection = ConnectionFactory().get_connection(connection_str=connection_str)
gkm_table = connection.get_or_create_store(settings.GKM_TABLE)
country_table = connection.get_or_create_store(settings.COUNTRY_TABLE)


def getgkm(model_name):
  """ returns gkm for a given model_name """
  return gkm_table.find_by_id(model_name)["gkm"]


def list_streams(streams_path):
  """ returns a list with the name of all streams available in the stream path """
  streams = []
  for f in os.listdir(streams_path):
    if os.path.islink(streams_path + f):
      if cluster.is_stream(streams_path + f):
        streams.append(f)
  return streams

def delete_streams(streams_path):
  """ delete all streams available in the stream path """
  try:
    for f in os.listdir(streams_path):
      if os.path.islink(streams_path + f):
        if cluster.is_stream(streams_path + f):
          cluster.delete_stream(streams_path + f)
    return True
  except:
    logging.ERROR(traceback.format_exc())
    return False

def delete_country(country=None):
  if country:
    country_table.delete(_id=country)
  else:
    connection.delete_store(settings.COUNTRY_TABLE)
    country_table = connection.get_or_create_store(settings.COUNTRY_TABLE)
  return True

# def update_consumers(): # Updates the active consumers
#   logging.debug("update consumers")
#   global consumers
#   streams = get_available_streams(streams_path)
  
#   # clean consumers
#   consumers_to_remove = []
#   for stream,consumer in consumers.items():
#     if stream not in streams:
#       consumers_to_remove.append(stream)
#   if len(consumers_to_remove):
#     # logging.debug("consumers to remove :")
#     # logging.debug(consumers_to_remove)
#     for consumer_to_remove in consumers_to_remove:
#       consumers[consumer_to_remove].close()
#       del consumers[consumer_to_remove]

  # # creating new consumers
  # for stream in streams:
  #   if not stream in consumers:
  #     logging.debug("subscribing to {}:{}".format(stream,"default_topic"))
  #     group = str(time.time())
  #     consumers[stream] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
  #     consumers[stream].subscribe([stream+":default_topic"])
  #     logging.debug("subscribed to {}:{}".format(stream,"default_topic"))
  # # logging.debug("Final consumers :")
  # # logging.debug(consumers)


# def update_global_consumers(): # Updates the active global consumers
#   global global_consumers

#   streams = get_global_streams()

#   # clean consumers
#   consumers_to_remove = []
#   for stream,consumer in global_consumers.items():
#     if stream not in streams:
#       consumers_to_remove.append(stream)
#   if len(consumers_to_remove):
#     # logging.debug("consumers to remove :")
#     # logging.debug(consumers_to_remove)
#     for consumer_to_remove in consumers_to_remove:
#       global_consumers[consumer_to_remove].close()
#       del global_consumers[consumer_to_remove]

#   # creating new consumers
#   for stream in streams:
#     if not stream in global_consumers:
#       logging.debug("subscribing to {}:{}".format(stream,"default_topic"))
#       group = str(time.time())
#       global_consumers[stream] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
#       global_consumers[stream].subscribe([stream+":default_topic"])
#       logging.debug("subscribed to {}:{}".format(stream,"default_topic"))


# def update_country_consumers(): # Updates the active global consumers
#   global country_consumers

#   streams = get_country_streams()

#   # clean consumers
#   consumers_to_remove = []
#   for stream,consumer in country_consumers.items():
#     if stream not in streams:
#       consumers_to_remove.append(stream)
#   if len(consumers_to_remove):
#     # logging.debug("consumers to remove :")
#     # logging.debug(consumers_to_remove)
#     for consumer_to_remove in consumers_to_remove:
#       country_consumers[consumer_to_remove].close()
#       del country_consumers[consumer_to_remove]

#   # creating new consumers
#   for stream in streams:
#     if not stream in country_consumers:
#       logging.debug("subscribing to {}:{}".format(stream,"default_topic"))
#       group = str(time.time())
#       country_consumers[stream] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
#       country_consumers[stream].subscribe([stream+":default_topic"])
#       logging.debug("subscribed to {}:{}".format(stream,"default_topic"))



app = Flask(__name__)



######  Web pages  #####

@app.route('/')
def home():
  return render_template('controller.html')

# @app.route('/dnd')
# def dnd():
#   return render_template('dnd.html')



######  AJAX functions  ######

# @app.route('/launch_carwatch',methods = ['POST'])
# def launch_carwatch(): # Launch carwatch for a given country
#   country = request.form["country"]
#   traffic = random.randint(10,100)
#   command_line = "python3 /mapr/" + cluster_name + "/demobdp2018/carwatch.py --country " + country + " --city " + country + " --traffic " + str(traffic) + " &"
#   os.system(command_line)
#   return "{} carwatch launched".format(country)


# @app.route('/get_stream_data',methods = ['POST'])
# def get_stream_data(): # Returns all stream data since last poll
#   logging.debug("get stream data")

#   # Variables definition
#   global consumers
#   cities = json.loads(request.form["cities"])
#   count = request.form["count"] == 'true'
#   consolidate = request.form["consolidate"] == 'true'

#   # logging.debug("variables :")
#   # logging.debug("cities : {}".format(cities))
#   # logging.debug("count : {}".format(count))
#   # logging.debug("consolidate : {}".format(consolidate))

#   # updating consumers to make sure we don't miss data
#   update_consumers()

#   # data results for each stage
#   raw_data = {}
#   count_data = {}
#   stream_data = {}

#   # Poll new vehicles from all the streams
#   for stream, consumer in consumers.items():
#     raw_data[stream] = {}
#     running = True
#     logging.debug("polling {}".format(stream))
#     while running:
#       msg = consumer.poll(timeout=1.0)
#       if msg is None:
#         running = False
#       else:
#         if not msg.error():
#           document = json.loads(msg.value().decode("utf-8"))
#           model = document["model"]
#           if model in raw_data[stream]:
#             raw_data[stream][model] += 1
#           else:
#             raw_data[stream][model] = 1
#         elif msg.error().code() != KafkaError._PARTITION_EOF:
#           print(msg.error())
#           running = False
#         else:
#           running = False
#   # logging.debug("raw data :")
#   # logging.debug(raw_data)

#   # format data
#   if consolidate:
#     count_data["Global"] = {}
#     for city,city_data in raw_data.items():
#       for k,v in city_data.items():
#         if k in count_data["Global"]:
#           count_data["Global"][k] += v
#         else:
#           count_data["Global"][k] = v
#   else:
#     for stream,data in raw_data.items():
#       count_data[stream.split('/')[-1]] = data

#   # logging.debug("count data : ")
#   # logging.debug(count_data)


#   # convert to gkm if required
#   if not count:
#     db = open_db()
#     gkm_table = open_table(db, GKM_TABLE_PATH)
#     for city,data in count_data.items():
#       stream_data[city]={}
#       for model_name,model_count in data.items():
#         gkm = getgkm(gkm_table,model_name)
#         if "gkm" in stream_data[city]:
#           stream_data[city]["gkm"] += gkm * model_count
#           stream_data[city]["count"] += model_count
#         else:
#           stream_data[city]["gkm"] = gkm * model_count
#           stream_data[city]["count"] = model_count
#   else:
#     stream_data = count_data
#   # logging.debug("stream data :")
#   # logging.debug(stream_data)

#   return json.dumps(stream_data)



###############################################


@app.route('/get_country_stream_data',methods = ['POST'])
def get_country_stream_data(): 
  """ Returns all stream data since last poll """
  global consumers
  # update_country_consumers()
  # data results for each stage
  raw_data = {}
  count_data = {}
  stream_data = {}

  # Poll new vehicles from all the streams
  for stream, consumer in consumers.items():
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
          running = False


  for stream,data in raw_data.items():
    count_data[stream.split('/')[-1]] = data

  stream_data = count_data


  return json.dumps(stream_data)






###############################################



# @app.route('/get_all_streams',methods = ['POST','GET'])
# def get_all_streams(): # Returns the list of all available stream names
#   return json.dumps(get_cities(streams_path))

@app.route('/get_source_streams',methods = ['POST'])
def get_source_streams():
  """ Returns the list of all existing streams in the data folder """
  return json.dumps(list_streams(settings.SOURCE_FOLDER))


@app.route('/deploy_new_country',methods=['POST'])
def deploy_country():
  global consumers
  new_country = request.form['country']
  try:
    country_port = country_table.find_by_id(new_country)["port"]
    return "Country already deployed"
  except:
    logging.info("deploying {}".format(new_country))
    subprocess.Popen(['python','traffic_generator.py','--country',new_country,'--traffic',str(random.randint(10,100))])
    country_table.insert_or_replace({"_id":new_country,"port":settings.COUNTRY_PORTS[new_country]})

    # Generating unique group ID to avoid collisions
    group = str(time.time())

    # Create a new consumer and store it in the "consumers" global variable
    consumers[new_country] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
    
    # Wait until the stream is ready
    stream_path = settings.SOURCE_FOLDER + new_country
    while not settings.CLUSTER.is_stream(stream_path):
      time.sleep(0.5)

    # subcribe to the default topic for the stream
    consumers[new_country].subscribe(["{}:default_topic".format(stream_path)])
    
    logging.info("{} deployed".format(new_country))
    return "{} deployed".format(new_country)



# @app.route('/get_deployed_countries',methods = ['GET'])
# def get_deployed_countries(): # Returns the list of all available stream names
#   deployed_countries = os.listdir("/mapr/" + cluster_name + "/countries/")
#   return json.dumps(deployed_countries)

# @app.route('/replicate_streams') 
# def replicate_streams():  # Replicate all stream to the stream_path directory
#   country_list = os.listdir("/mapr/" + cluster_name + "/countries/")
#   logging.debug("Country list :")
#   logging.debug(country_list)
#   for country in country_list:
#     logging.debug("replicating streams from {}".format(country))
#     command_line = '/mapr/' + cluster_name + '/demobdp2018/replicateStream.sh -s /mapr/' + cluster_name + '/countries/' + country + '/streams/ -t ' + streams_path
#     logging.debug("command line : ")
#     logging.debug(command_line)
#     os.system(command_line)
#   return "Done"



# @app.route('/get_events_count')
# def get_events_count(): # Returns the number of events in the raw db
#   db = open_db()
#   count_table = open_table(db, COUNT_TABLE_PATH)
#   try:
#     event_count = count_table.find_by_id("total_count")["count"]
#   except:
#     event_count = 0
#   return json.dumps({"count":event_count})

# @app.route('/get_countries')
# def get_countries(): # Returns the number of events in the raw db
#   countries = []
#   for c in country_table.find():
#     countries.append(c)
#   # logging.debug(countries)
#   return json.dumps({"countries":countries})



@app.route('/remove_country',methods=['POST'])
def remove_country():
  global consumers

  # get country to delete from the request
  country = request.form['country']
  
  # Remove the country from the countries db
  country_table.delete(_id=country)

  # Delete the consumer from the global list of active consumers
  del consumers[country]

  # Deletes the stream on the cluster
  settings.CLUSTER.delete_stream(settings.SOURCE_FOLDER + country)
  

  return "{} removed".format(country)


"""
# Start existing countries
db = open_db()
country_table = open_table(db, COUNTRIES_TABLE_PATH)
for c in country_table.find():
  country = c["_id"]
  port = c["port"]
  logging.debug("Starting localfront for {}".format(country))
  command_line = "python3 /mapr/" + cluster_name + "/demobdp2018/localfront.py --country " + country + " --port " + str(port) + " &"
  os.system(command_line)
"""


if __name__ == '__main__':
# update_consumers()
  # Deletes all existing streams at startup
  delete_streams(settings.SOURCE_FOLDER)
  delete_streams(settings.TARGET_FOLDER)
  delete_country()
  app.run(debug=False,host='0.0.0.0',port=80)
# for k,c in consumers.items():
#   c.close()

