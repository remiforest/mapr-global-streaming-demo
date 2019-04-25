#! /usr/bin/python

"""

Settings files for mapr-global-streaming-demo project


"""

import maprutils
import mapr_manager

# Authentication settings. 
SECURE_MODE = False
USERNAME = "mapr"
PASSWORD = "mapr"
PEM_FILE = "/opt/mapr/conf/ssl_truststore.pem"


# Cluster information
CLUSTER_NAME = maprutils.cluster.get_name()
CLUSTER_IP = maprutils.cluster.get_ip()[0]
CLUSTER = mapr_manager.Cluster(CLUSTER_NAME,CLUSTER_IP,USERNAME,PASSWORD)

# Project folders
PROJECT_FOLDER = "/mapr-global-streaming-demo/" # Project folder from the cluster root
ROOT_PATH = '/mapr/' + CLUSTER_NAME + PROJECT_FOLDER
LOG_FOLDER = ROOT_PATH + "logs/" # Folder to store the logs
DATA_FOLDER = ROOT_PATH + 'data/'   # Folder to store the data (streams and tables)
SOURCE_FOLDER = DATA_FOLDER + 'source/'   # Folder to store the data (streams and tables)
TARGET_FOLDER = DATA_FOLDER + 'target/'   # Folder to store the data (streams and tables)
GKM_TABLE = DATA_FOLDER + 'cargkm'  # Path for the table that stores GKM information
COUNT_TABLE = DATA_FOLDER + 'count'  # Path for the table that stores count information
COUNTRY_TABLE = DATA_FOLDER + 'country'  # Path for the table that stores countries information


# App settings
COUNTRY_PORTS = {"north-america":"8080",
                 "europe":"8081",
                 "australia":"8082"}

