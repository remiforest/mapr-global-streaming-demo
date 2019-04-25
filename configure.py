#! /usr/bin/python

"""
Configure the application before starting

"""

import fileinput
import os
import settings
import time
import subprocess
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

print("Starting pre-flight checks ... ")

# Create folders
if not os.path.exists(settings.DATA_FOLDER):
    os.makedirs(settings.DATA_FOLDER)
if not os.path.exists(settings.SOURCE_FOLDER):
    os.makedirs(settings.SOURCE_FOLDER)
if not os.path.exists(settings.TARGET_FOLDER):
    os.makedirs(settings.TARGET_FOLDER)


print("Data directory created")


if not os.path.exists(settings.LOG_FOLDER):
    os.makedirs(settings.LOG_FOLDER)

print("Log directory created")


print("updating init file")
os.system("sed -i 's/demo\.mapr\.com/{}/g' init.sh".format(settings.CLUSTER_NAME))

print("Configuration complete, initialize environment variables with source init.sh then run the aplication using start.py")
