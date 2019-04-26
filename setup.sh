#!/usr/bin/env bash

# Install the prerequisites for running the demo

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi
if [ -n "$(command -v yum)" ];
then
    yum install -y gcc-c++
    yum install -y python-setuptools
    yum install -y python-pip
fi
if [ -n "$(command -v apt-get)" ];
then
    apt-get install -y gcc
    apt-get install -y python-setuptools
    apt-get install -y python-pip
fi

[ -n "$(command -v yum)" ]

[ -n "$(command -v apt-get)" ]


pip install --upgrade pip

pip install git+https://github.com/remiforest/mapr-manager.git --upgrade
pip install git+https://github.com/remiforest/maprutils.git --upgrade
pip install maprdb-python-client
pip install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" mapr-streams-python
pip install flask

echo -e "\n\n\n Run python configure.py to configure the application \n\n\n"

