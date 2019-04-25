#!/usr/bin/env bash

# Install the prerequisites for running the demo

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

yum install python-setuptools -y
yum install -y python-pip

pip install --upgrade pip

pip install git+https://github.com/remiforest/mapr-manager.git --upgrade
pip install git+https://github.com/remiforest/maprutils.git --upgrade
pip install maprdb-python-client
pip install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" mapr-streams-python
pip install flask

echo -e "\n\n\n Run python configure.py to configure the application \n\n\n"

