# mapr-global-streaming-demo

This demo show the live stream replication features integrated in the MapR data platform

Prerequisites
A MapR cluster in version 6.1 with DB and streams enabled, as well as the mapr-gateway service for stream replication.

The repo has to be deployed on the MapR file system (under /mapr)

<h3>Setup</h3>
edit settings.py to update username, password and project path.
launch the setup script as root to deploy all prerequisites : (sudo) ./setup.sh 
run python configure.py to create all directories
run source init.sh to initialize LD_LIBRARY_PATH
launch the application with python controller.py

then open the URL of the node you deployed your application on.


